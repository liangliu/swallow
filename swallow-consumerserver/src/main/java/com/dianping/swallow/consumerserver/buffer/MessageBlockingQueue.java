package com.dianping.swallow.consumerserver.buffer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.message.Message;

public final class MessageBlockingQueue extends LinkedBlockingQueue<Message> {

   private static final long           serialVersionUID = -633276713494338593L;
   private static final Logger         LOG              = LoggerFactory.getLogger(MessageBlockingQueue.class);

   private final String                cid;
   private final String                topicName;
   private final MessageRetriverThread messageRetriverThread;

   /** 最小剩余数量,当queue的消息数量小于threshold时，会触发从数据库加载数据的操作 */
   private final int                   threshold;

   protected MessageRetriever          messageRetriever;

   private ReentrantLock               reentrantLock    = new ReentrantLock();
   private Condition                   condition        = reentrantLock.newCondition();

   protected volatile Long             tailMessageId;
   protected MessageFilter messageFilter;

   public MessageBlockingQueue(String cid, String topicName, int threshold, int capacity, Long messageIdOfTailMessage) {
      super(capacity);
      //能运行到这里，说明capacity>0
      this.cid = cid;
      this.topicName = topicName;
      if (threshold < 0)
         throw new IllegalArgumentException("threshold: " + threshold);
      this.threshold = threshold;
      if (messageIdOfTailMessage == null)
         throw new IllegalArgumentException("messageIdOfTailMessage is null.");
      this.tailMessageId = messageIdOfTailMessage;
      messageRetriverThread = new MessageRetriverThread();
      messageRetriverThread.start();
      //Hawk监控
      HawkJMXUtil.registerMBean(topicName + "-" + cid + "-MessageBlockingQueue", new HawkMBean());
   }

   public MessageBlockingQueue(String cid, String topicName, int threshold, int capacity, Long messageIdOfTailMessage,
                               MessageFilter messageFilter) {
      super(capacity);
      //能运行到这里，说明capacity>0
      this.cid = cid;
      this.topicName = topicName;
      if (threshold < 0)
         throw new IllegalArgumentException("threshold: " + threshold);
      this.threshold = threshold;
      if (messageIdOfTailMessage == null)
         throw new IllegalArgumentException("messageIdOfTailMessage is null.");
      this.tailMessageId = messageIdOfTailMessage;
      this.messageFilter = messageFilter;
      messageRetriverThread = new MessageRetriverThread();
      messageRetriverThread.start();
      //Hawk监控
      HawkJMXUtil.registerMBean(topicName + "-" + cid + "-MessageBlockingQueue", new HawkMBean());
   }

   public Message take() throws InterruptedException {
      throw new UnsupportedOperationException(
            "Don't call this operation, call 'poll(long timeout, TimeUnit unit)' instead.");
   }

   public Message poll() {
      //如果剩余元素数量小于最低限制值threshold，就启动一个“获取DB数据的后台线程”去DB获取数据，并添加到Queue的尾部
      if (super.size() < threshold) {
         ensureLeftMessage();
      }
      return super.poll();
   }

   public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
      //如果剩余元素数量小于最低限制值threshold，就启动一个“获取DB数据的后台线程”去DB获取数据，并添加到Queue的尾部
      if (super.size() < threshold) {
         ensureLeftMessage();
      }
      return super.poll(timeout, unit);
   }

   /**
    * 启动一个“获取DB数据的后台线程”去DB获取数据，并添加到Queue的尾部
    */
   private void ensureLeftMessage() {
      if (messageRetriever == null) {
         return;
      }
      //只有一个线程能唤醒“获取DB数据的后台线程”
      if (reentrantLock.tryLock()) {
         condition.signal();
         reentrantLock.unlock();
      }
   }

   public void setMessageRetriever(MessageRetriever messageRetriever) {
      this.messageRetriever = messageRetriever;
   }

   /**
    * 关闭该对象绑定的线程
    */
   @Override
   protected void finalize() throws Throwable {
      super.finalize();
      //关闭该对象绑定的线程
      this.messageRetriverThread.interrupt();
      LOG.info("Called finalize() of MessageBlockingQueue: " + this);
   }

   private class MessageRetriverThread extends Thread {

      public MessageRetriverThread() {
         this.setName("MessageRetriever-(topic=" + topicName + ",cid=" + cid + ")");
         this.setDaemon(true);
      }

      @Override
      public void run() {
         LOG.info("thread start:" + this.getName());
         while (!this.isInterrupted()) {
            reentrantLock.lock();
            try {
               condition.await();
               retriveMessage();
            } catch (InterruptedException e) {
               this.interrupt();
            } finally {
               reentrantLock.unlock();
            }
         }
         LOG.info("thread done:" + this.getName());
      }

      private void retriveMessage() {
         if (LOG.isDebugEnabled()) {
            LOG.debug("retriveMessage() start:" + this.getName());
         }
         try {
            List<Message> messages = messageRetriever.retriveMessage(MessageBlockingQueue.this.topicName,
                  MessageBlockingQueue.this.tailMessageId, MessageBlockingQueue.this.messageFilter);
            if (messages != null) {
               int size = messages.size();
               for (int i = 0; i < size; i++) {
                  Message message = messages.get(i);
                  try {
                     MessageBlockingQueue.this.put(message);
                     Long messageId = message.getMessageId();
                     if (messageId == null) {
                        throw new IllegalStateException("the retrived message's messageId is null:" + message);
                     }
                     tailMessageId = messageId;
                     if (LOG.isDebugEnabled()) {
                        LOG.debug("add message to (topic=" + topicName + ",cid=" + cid + ") queue:"
                              + message.toString());
                     }
                  } catch (InterruptedException e) {
                     this.interrupt();
                     break;
                  }
               }
            }
         } catch (Exception e1) {
            LOG.error(e1.getMessage(), e1);
         }
         if (LOG.isDebugEnabled()) {
            LOG.debug("retriveMessage() done:" + this.getName());
         }
      }
   }

   //以下是供Hawk监控使用的MBean
   public class HawkMBean {

      public String getCid() {
         return cid;
      }

      public String getTopicName() {
         return topicName;
      }

      public int getThreshold() {
         return threshold;
      }

      public Long getTailMessageId() {
         return tailMessageId;
      }

      public MessageFilter getMessageFilter() {
         return messageFilter;
      }

      public int getSize() {
         return size();
      }

      public int getRemainingCapacity() {
         return remainingCapacity();
      }

      public String getMessageRetriverThreadStatus() {
         return messageRetriverThread.getState().toString();
      }
   }

}
