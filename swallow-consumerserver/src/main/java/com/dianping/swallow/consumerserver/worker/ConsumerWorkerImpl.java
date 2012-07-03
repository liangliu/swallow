package com.dianping.swallow.consumerserver.worker;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.consumer.ACKHandlerType;
import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.dao.impl.mongodb.MongoUtils;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.threadfactory.MQThreadFactory;
import com.dianping.swallow.consumerserver.GetMessageThread;
import com.dianping.swallow.consumerserver.HandleACKThread;
import com.dianping.swallow.consumerserver.buffer.SwallowBuffer;

public class ConsumerWorkerImpl implements ConsumerWorker {
   private static final Logger          LOG                   = LoggerFactory.getLogger(ConsumerWorkerImpl.class);

   private ConsumerInfo                 consumerInfo;
   private BlockingQueue<Channel>       freeChannels          = new ArrayBlockingQueue<Channel>(10);
   private Set<Channel>                 connectedChannels     = new HashSet<Channel>();
   private ArrayBlockingQueue<Runnable> ackWorker             = new ArrayBlockingQueue<Runnable>(10);
   private boolean                      getMessageThreadExist = Boolean.FALSE;
   private boolean                      handleACKThreadExist  = Boolean.FALSE;
   private BlockingQueue<Message>       messageQueue          = null;
   private AckDAO                       ackDao;
   private SwallowBuffer                swallowBuffer;
   private MessageDAO                   messageDao;
   private PktMessage                   preparedMessage       = null;
   private SwallowMessage               message;
   private long                         getMessageInterval    = 1000L;
   private MQThreadFactory              threadFactory;
   private String                       consumerid;
   private String                       topicName;
   private volatile boolean             handleACKIsAlive               = true;
   private volatile boolean             getMessageisAlive              = true;

   public Set<Channel> getConnectedChannels() {
      return connectedChannels;
   }

   public void setGetMessageThreadExist(boolean getMessageThreadExist) {
      this.getMessageThreadExist = getMessageThreadExist;
   }

   public void setHandleACKThreadExist(boolean handleACKThreadExist) {
      this.handleACKThreadExist = handleACKThreadExist;
   }

   public ConsumerWorkerImpl(ConsumerInfo consumerInfo, AckDAO ackDao, MessageDAO messageDao,
                             SwallowBuffer swallowBuffer, MQThreadFactory threadFactory) {
      this.consumerInfo = consumerInfo;
      this.ackDao = ackDao;
      this.messageDao = messageDao;
      this.swallowBuffer = swallowBuffer;
      this.threadFactory = threadFactory;
      topicName = consumerInfo.getConsumerId().getDest().getName();
      consumerid = consumerInfo.getConsumerId().getConsumerId();
   }

   @Override
   public void handleAck(final Channel channel, final Long ackedMsgId, final ACKHandlerType type) {

      ackWorker.add(new Runnable() {
         @Override
         public void run() {
            updateMaxMessageId(ackedMsgId);
            if (ACKHandlerType.CLOSE_CHANNEL.equals(type)) {
               handleChannelDisconnect(channel);
            } else if (ACKHandlerType.SEND_MESSAGE.equals(type)) {
               freeChannels.add(channel);
            }
         }
      });

   }

   private void updateMaxMessageId(Long ackedMsgId) {
      if (ackedMsgId != null && ConsumerType.AT_LEAST.equals(consumerInfo.getConsumerType())) {
         ackDao.add(topicName, consumerid, ackedMsgId);
      }
   }

   @Override
   public void handleChannelDisconnect(Channel channel) {
      synchronized (connectedChannels) {
         connectedChannels.remove(channel);
      }
   }

   private void getMessageLoop() {
      while (getMessageisAlive) {
         sendMessageByPollFreeChannelQueue();
         synchronized (getConnectedChannels()) {
            if (getConnectedChannels().isEmpty()) {
               setGetMessageThreadExist(false);
               getMessageisAlive = false;
            }
         }

      }
      LOG.info("closed");
   }

   private void handleAckLoop() {
      ArrayBlockingQueue<Runnable> ackWorker = getAckWorker();
      while (handleACKIsAlive) {
         Runnable worker = null;
         try {
            while (true) {
               worker = ackWorker.poll(1000, TimeUnit.MILLISECONDS);// TODO 
               if (worker != null) {
                  worker.run();
               } else {
                  break;
               }
            }
         } catch (InterruptedException e) {
            LOG.error("unexpected interrupt", e);
         }
         synchronized (getConnectedChannels()) {
            if (getConnectedChannels().isEmpty()) {
               setHandleACKThreadExist(false);
               handleACKIsAlive = false;
            }
         }

      }
      LOG.info("closed");
   }

   private void newHandleACKThread() {

      Thread thread2 = threadFactory.newThread(new Runnable() {

         @Override
         public void run() {
            handleAckLoop();

         }
      }, consumerInfo.toString() + "-handleACKThread-");
      thread2.start();
   }

   private void newGetMessageThread() {

      Thread thread1 = threadFactory.newThread(new Runnable() {

         @Override
         public void run() {
            getMessageLoop();

         }
      }, consumerInfo.toString() + "-getMessageThread-");
      thread1.start();

   }

   @Override
   public void handleGreet(final Channel channel, final int clientThreadCount) {
      ackWorker.add(new Runnable() {
         @Override
         public void run() {
            synchronized (connectedChannels) {
               connectedChannels.add(channel);
            }
            for(int i = 0; i < clientThreadCount; i++){
               freeChannels.add(channel);
            }
            
            if (!getMessageThreadExist) {
               newGetMessageThread();
               getMessageThreadExist = Boolean.TRUE;
            }
         }
      });
      if (!handleACKThreadExist) {
         newHandleACKThread();
         handleACKThreadExist = Boolean.TRUE;
      }

   }

   @Override
   public void close() {
      getMessageisAlive = false;
      try {
         Thread.sleep(20000);
      } catch (InterruptedException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      handleACKIsAlive = false;
   }

   public ArrayBlockingQueue<Runnable> getAckWorker() {
      return ackWorker;
   }

   public void setAckWorker(ArrayBlockingQueue<Runnable> ackWorker) {
      this.ackWorker = ackWorker;
   }

   private long getMessageIdOfTailMessage(String topicName, String consumerId) {

      Long maxMessageId = ackDao.getMaxMessageId(topicName, consumerId);
      if (maxMessageId == null) {
         maxMessageId = messageDao.getMaxMessageId(topicName);
      }
      if (maxMessageId == null) {
         int time = (int) (System.currentTimeMillis() / 1000);
         time = time - 3600 * 24;
         BSONTimestamp bst = new BSONTimestamp(time, 1);
         maxMessageId = MongoUtils.BSONTimestampToLong(bst);
      }
      return maxMessageId;
   }

   @Override
   public void sendMessageByPollFreeChannelQueue() {
      if (messageQueue == null) {
         long messageIdOfTailMessage = getMessageIdOfTailMessage(topicName, consumerid);
         messageQueue = swallowBuffer.createMessageQueue(topicName, consumerid, messageIdOfTailMessage);
      }
      //线程刚起，第一次调用的时候，需要先去mongo中获取maxMessageId
      try {
         while (getMessageisAlive) {
            Channel channel = null;
            synchronized (freeChannels) {
               if (freeChannels == null) {
                  break;
               }
               channel = freeChannels.poll(1000, TimeUnit.MILLISECONDS);// TODO
            }
            if (channel == null) {
               break;

            } else if (channel.isConnected()) {
               if (preparedMessage == null) {
                  while (getMessageisAlive) {
                     //获得
                     message = (SwallowMessage) messageQueue.poll(getMessageInterval, TimeUnit.MILLISECONDS);
                     if (message == null) {
                        getMessageInterval *= 2;
                     } else {
                        getMessageInterval = 1000;
                        break;
                     }
                  }
                  preparedMessage = new PktMessage(consumerInfo.getConsumerId().getDest(), message);
               }
               //收到close命令后,可能没有取得消息,此时,message为null,不做任何事情.此线程结束.
               if (message != null) {
                  Long messageId = message.getMessageId();
                  //如果consumer是收到ACK之前更新messageId的类型
                  if (ConsumerType.AT_MOST.equals(consumerInfo.getConsumerType())) {
                     ackDao.add(topicName, consumerid, messageId);
                  }
                  if (channel.isConnected()) {
                     channel.write(preparedMessage);
                     preparedMessage = null;
                  }
               }

            }
         }
      } catch (InterruptedException e) {
         LOG.error("thread InterruptedException", e);
      }
   }
}
