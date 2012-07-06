package com.dianping.swallow.consumerserver.worker;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.consumer.ACKHandlerType;
import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.dao.impl.mongodb.MongoUtils;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.threadfactory.DefaultPullStrategy;
import com.dianping.swallow.common.threadfactory.MQThreadFactory;
import com.dianping.swallow.common.threadfactory.PullStrategy;
import com.dianping.swallow.consumerserver.buffer.SwallowBuffer;
import com.dianping.swallow.consumerserver.config.ConfigManager;

public class ConsumerWorkerImpl implements ConsumerWorker {
   private static final Logger                    LOG               = LoggerFactory.getLogger(ConsumerWorkerImpl.class);

   private ConsumerInfo                           consumerInfo;
   private BlockingQueue<Channel>                 freeChannels      = new LinkedBlockingQueue<Channel>();
   private Set<Channel>                           connectedChannels = Collections
                                                                          .synchronizedSet(new HashSet<Channel>());
   private BlockingQueue<Message>                 messageQueue      = null;
   private AckDAO                                 ackDao;
   private SwallowBuffer                          swallowBuffer;
   private MessageDAO                             messageDao;
   private Queue<PktMessage>                      cachedMessages    = new ConcurrentLinkedQueue<PktMessage>();
   private MQThreadFactory                        threadFactory;
   private String                                 consumerid;
   private String                                 topicName;
   private volatile boolean                       getMessageisAlive = true;
   private volatile boolean                       started           = false;
   private ExecutorService                        ackExecutor;
   private ConsumerWorkerManager                  workerManager;
   private PullStrategy                           pullStgy;
   private ConfigManager                          configManager;
   private Map<Channel, Map<PktMessage, Boolean>> waitAckMessages   = new ConcurrentHashMap<Channel, Map<PktMessage, Boolean>>();

   public Set<Channel> getConnectedChannels() {
      return connectedChannels;
   }

   public ConsumerWorkerImpl(ConsumerInfo consumerInfo, ConsumerWorkerManager workerManager) {
      this.consumerInfo = consumerInfo;
      this.configManager = workerManager.getConfigManager();
      this.ackDao = workerManager.getAckDAO();
      this.messageDao = workerManager.getMessageDAO();
      this.swallowBuffer = workerManager.getSwallowBuffer();
      this.threadFactory = workerManager.getThreadFactory();
      topicName = consumerInfo.getConsumerId().getDest().getName();
      consumerid = consumerInfo.getConsumerId().getConsumerId();
      this.workerManager = workerManager;
      pullStgy = new DefaultPullStrategy(configManager.getPullFailDelayBase(),
            configManager.getPullFailDelayUpperBound());

      ackExecutor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

      startMessageFetcherThread();
      startConnectedChannelCheckerThread();

      //Hawk监控
      HawkJMXUtil.registerMBean(topicName + '-' + consumerid + "-ConsumerWorkerImpl", new HawkMBean());
   }

   private void startConnectedChannelCheckerThread() {
      threadFactory.newThread(new Runnable() {
         @Override
         public void run() {
            while (getMessageisAlive) {
               if (started) {
                  if (connectedChannels.isEmpty()) {
                     getMessageisAlive = false;
                     ackExecutor.shutdownNow();
                     workerManager.workerDone(consumerInfo.getConsumerId());
                     LOG.info("ConsumerWorker for " + consumerInfo.getConsumerId()
                           + " has no connected channel, close it");
                     break;
                  }
               }
               try {
                  Thread.sleep(configManager.getCheckConnectedChannelInterval());
               } catch (InterruptedException e) {
                  break;
               }
            }
            LOG.info("connected channel checker thread closed");
         }
      }, consumerInfo.toString() + "swallow-connectedChannelChecker-").start();

   }

   @Override
   public void handleAck(final Channel channel, final Long ackedMsgId, final ACKHandlerType type) {

      ackExecutor.execute(new Runnable() {
         @Override
         public void run() {
            while (true) {
               try {
                  updateWaitAckMessages(channel, ackedMsgId);
                  updateMaxMessageId(ackedMsgId);
                  break;
               } catch (Exception e) {
                  LOG.error("updateMaxMessageId wrong!", e);
                  try {
                     Thread.sleep(configManager.getRetryIntervalWhenMongoException());
                  } catch (InterruptedException e1) {
                     break;
                  }
               }
            }
            if (ACKHandlerType.CLOSE_CHANNEL.equals(type)) {
               handleChannelDisconnect(channel);
            } else if (ACKHandlerType.SEND_MESSAGE.equals(type)) {
               freeChannels.add(channel);
            }
         }
      });

   }

   private void updateWaitAckMessages(Channel channel, Long ackedMsgId) {
      if (ConsumerType.AT_LEAST.equals(consumerInfo.getConsumerType())) {
         Map<PktMessage, Boolean> messages = waitAckMessages.get(channel);
         SwallowMessage swallowMsg = new SwallowMessage();
         swallowMsg.setMessageId(ackedMsgId);
         PktMessage mockPktMessage = new PktMessage(consumerInfo.getConsumerId().getDest(), swallowMsg);
         messages.remove(mockPktMessage);
      }

   }

   private void updateMaxMessageId(Long ackedMsgId) {
      if (ackedMsgId != null && ConsumerType.AT_LEAST.equals(consumerInfo.getConsumerType())) {
         ackDao.add(topicName, consumerid, ackedMsgId);
      }
   }

   @Override
   public void handleChannelDisconnect(Channel channel) {
      connectedChannels.remove(channel);
      if (ConsumerType.AT_LEAST.equals(consumerInfo.getConsumerType())) {
         Map<PktMessage, Boolean> messageMap = waitAckMessages.get(channel);
         if (messageMap != null) {
            for (Map.Entry<PktMessage, Boolean> messageEntry : messageMap.entrySet()) {
               cachedMessages.add(messageEntry.getKey());

            }
         }
      }

   }

   private void startMessageFetcherThread() {

      threadFactory.newThread(new Runnable() {

         @Override
         public void run() {
            while (getMessageisAlive) {
               sendMessageByPollFreeChannelQueue();
            }
            LOG.info("message fetcher thread closed");
         }
      }, consumerInfo.toString() + "-messageFetcher-").start();

   }

   @Override
   public void handleGreet(final Channel channel, final int clientThreadCount) {
      ackExecutor.execute(new Runnable() {
         @Override
         public void run() {
            connectedChannels.add(channel);
            started = true;
            for (int i = 0; i < clientThreadCount; i++) {
               freeChannels.add(channel);
            }
         }
      });
   }

   public void closeMessageFetcherThread() {
      getMessageisAlive = false;
   }

   public void closeAckExecutor() {
      ackExecutor.shutdownNow();
   }

   public void close() {
      getMessageisAlive = false;

   }

   private long getMessageIdOfTailMessage(String topicName, String consumerId) {

      Long maxMessageId = ackDao.getMaxMessageId(topicName, consumerId);
      if (maxMessageId == null) {
         maxMessageId = messageDao.getMaxMessageId(topicName);
      }
      if (maxMessageId == null) {
         int time = (int) (System.currentTimeMillis() / 1000);
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
            Channel channel = freeChannels.take();
            if (channel.isConnected()) {

               if (cachedMessages.size() == 0) {
                  SwallowMessage message = null;
                  while (getMessageisAlive) {
                     //从blockQueue中获取消息
                     message = (SwallowMessage) messageQueue.poll(pullStgy.fail(false), TimeUnit.MILLISECONDS);
                     if (message != null) {
                        pullStgy.succeess();
                        break;
                     }
                  }
                  if (message != null) {
                     cachedMessages.add(new PktMessage(consumerInfo.getConsumerId().getDest(), message));
                  }
               }
               //收到close命令后,可能没有取得消息,此时,message为null,不做任何事情.此线程结束.
               if (cachedMessages.size() != 0) {
                  PktMessage preparedMessage = cachedMessages.poll();
                  Long messageId = preparedMessage.getContent().getMessageId();
                  //如果consumer是收到ACK之前更新messageId的类型
                  if (ConsumerType.AT_MOST.equals(consumerInfo.getConsumerType())) {
                     while (true) {
                        try {
                           ackDao.add(topicName, consumerid, messageId);
                           break;
                        } catch (Exception e) {
                           LOG.error("ackDao.add wrong!", e);
                           Thread.sleep(configManager.getRetryIntervalWhenMongoException());
                        }
                     }
                  }
                  try {
                     channel.write(preparedMessage);
                     if (ConsumerType.AT_LEAST.equals(consumerInfo.getConsumerType())) {
                        Map<PktMessage, Boolean> messageMap = waitAckMessages.get(channel);
                        if (messageMap == null) {
                           messageMap = new ConcurrentHashMap<PktMessage, Boolean>();
                           waitAckMessages.put(channel, messageMap);
                        }
                        messageMap.put(preparedMessage, Boolean.TRUE);
                     }
                  } catch (Exception e) {
                     LOG.error(consumerInfo.toString() + "：channel write error.", e);
                     cachedMessages.add(preparedMessage);
                  }
               }

            }
         }
      } catch (InterruptedException e) {
         LOG.info("get message from messageQueue thread InterruptedException", e);
      }
   }

   /**
    * 用于Hawk监控
    */
   public class HawkMBean {
      public String getConnectedChannels() {
         StringBuilder sb = new StringBuilder();
         if (connectedChannels != null) {
            for (Channel channel : connectedChannels) {
               sb.append(channel.getRemoteAddress()).append("(isConnected:").append(channel.isConnected()).append(')');
            }
         }
         return sb.toString();
      }

      public String getFreeChannels() {
         StringBuilder sb = new StringBuilder();
         if (freeChannels != null) {
            for (Channel channel : freeChannels) {
               sb.append(channel.getRemoteAddress()).append("(isConnected:").append(channel.isConnected()).append(')');
            }
         }
         return sb.toString();
      }

      public String getConsumerInfo() {
         return "ConsumerId=" + consumerInfo.getConsumerId() + ",ConsumerType=" + consumerInfo.getConsumerType();
      }

      public String getConsumerid() {
         return consumerid;
      }

      public String getTopicName() {
         return topicName;
      }

      public String getPreparedMessage() {
         if (cachedMessages != null) {
            return cachedMessages.toString();
         }
         return null;
      }

      public boolean isGetMessageisAlive() {
         return getMessageisAlive;
      }

      public boolean isStarted() {
         return started;
      }

   }

}
