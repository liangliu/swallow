package com.dianping.swallow.consumerserver.buffer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.message.Message;

public final class MessageBlockingQueue extends LinkedBlockingQueue<Message> implements CloseableBlockingQueue<Message> {

   private static final long           serialVersionUID = -633276713494338593L;
   private static final Logger         LOG              = LoggerFactory.getLogger(MessageBlockingQueue.class);

   private final String                cid;
   private final String                topicName;
   private final MessageRetriverThread messageRetriverThread;

   /** 最小剩余数量,当queue的消息数量小于threshold时，会触发从数据库加载数据的操作 */
   private final int                   threshold;

   protected MessageRetriever          messageRetriever;

   private ReentrantLock               reentrantLock    = new ReentrantLock(true);
   private Condition                   condition        = reentrantLock.newCondition();

   protected volatile Long             tailMessageId;
   protected MessageFilter             messageFilter;

   private AtomicBoolean               isClosed         = new AtomicBoolean(false);

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
      //阻塞take()方法不让使用的原因是：防止当ensureLeftMessage()失败时，没法获取消息，而调用take()的代码将一直阻塞。
      //不过，可以在后台线程中获取消息失败时不断重试，直到保证ensureLeftMessage。
      //但是极端情况下，可能出现：
      //  后台线程某时刻判断size()是足够的，所以wait；但在wait前，queue的元素又被取光了，外部调用者继续在take()上阻塞；而此时后台线程也wait，就‘死锁’了。
      //  即，size()的判断和fetch消息的操作，比较作为同步块原子化。才能从根本上保证线程安全。
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
    * 唤醒“获取DB数据的后台线程”去DB获取数据，并添加到Queue的尾部
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

   private class MessageRetriverThread extends Thread {

      public MessageRetriverThread() {
         this.setName("swallow-MessageRetriever-(topic=" + topicName + ",cid=" + cid + ")");
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

   /**
    * 关闭BlockingQueue占用的资源。<br>
    * <note> 注意:close()只是中断了内部的后台线程，并不会导致poll()和poll(long timeout, TimeUnit
    * unit)方法不可用,您依然可以使用poll()和poll(long timeout, TimeUnit
    * unit)方法获取MessageBlockingQueue中剩余的消息。不过，不会再有后台线程去从DB获取更多的消息。 </note>
    */
   public void close() {
      if (isClosed.compareAndSet(false, true)) {
         this.messageRetriverThread.interrupt();
      }
   }

   public void isClosed() {
      if (isClosed.get()) {
         throw new RuntimeException("MessageBlockingQueue- already closed! ");
      }
   }

}
