package com.dianping.swallow.common.buffer;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import com.dianping.swallow.common.message.Message;

public class TopicBuffer implements Serializable {

   private static final long                               serialVersionUID                 = 8764667102499944469L;

   /**
    * 锁是为了防止相同topicName的TopicBuffer被并发创建，所以相同的topicName对应相同的锁。
    * 锁的个数也决定了最多能有多少创建TopicBuffer的并发操作
    */
   private static final int                                LOCK_NUM_FOR_CREATE_TOPIC_BUFFER = 10;
   private static ReentrantLock[]                          locksForCreateTopicBuffer        = new ReentrantLock[LOCK_NUM_FOR_CREATE_TOPIC_BUFFER];
   static {
      for (int i = 0; i < locksForCreateTopicBuffer.length; i++) {
         locksForCreateTopicBuffer[i] = new ReentrantLock();
      }
   }
   private static final ConcurrentMap<String, TopicBuffer> topicBuffers                     = new ConcurrentHashMap<String, TopicBuffer>();

   private final String                                    topicName;

   private ConcurrentHashMap<Long, BlockingQueue<Message>> messageQueues                    = new ConcurrentHashMap<Long, BlockingQueue<Message>>();

   private TopicBuffer(String topicName) {
      this.topicName = topicName;
   }

   /**
    * 根据topicName，获取topicName对应的TopicBuffer。<br>
    * 如果topicBuffer已经存在，则返回。否则创建一个新的TopicBuffer，该方法保证，
    * 一个topicName只有一个TopicBuffer。<br>
    * 该方法是线程安全的。
    */
   public static TopicBuffer getTopicBuffer(String topicName) {
      TopicBuffer topicBuffer = topicBuffers.get(topicName);
      if (topicBuffer != null) {
         return topicBuffer;
      }
      // topicBuffer不存在，须创建
      ReentrantLock reentrantLock = locksForCreateTopicBuffer[index(topicName)];
      try {
         reentrantLock.lock();// 加锁，防止同时创建相同topicName的TopicCache
         topicBuffer = topicBuffers.get(topicName);// double check
         if (topicBuffer == null) {
            topicBuffer = new TopicBuffer(topicName);
            topicBuffers.put(topicName, topicBuffer);
         }
      } finally {// 释放锁
         reentrantLock.unlock();
      }
      return topicBuffer;
   }

   /**
    * 根据消费者id，获取消息队列<br>
    * 该方法是线程安全的。
    * 
    * @param cid 消费者id
    * @return 返回消费者id对应的消息队列
    */
   public BlockingQueue<Message> getMessageQueue(long cid) {
      return messageQueues.get(cid);
   }

   public BlockingQueue<Message> createMessageQueue(long cid, long messageIdOfTailMessage) {
      MessageBlockingQueue messageBlockingQueue;
      //TODO 通过lion获取参数
      int threshold = 10;
      int capacity = 100;
      if (capacity < 0) {
         messageBlockingQueue = new MessageBlockingQueue(cid, this.topicName, threshold, messageIdOfTailMessage);
      } else {
         messageBlockingQueue = new MessageBlockingQueue(cid, this.topicName, threshold, capacity,
               messageIdOfTailMessage);
      }
      messageBlockingQueue.setMessageRetriever(new MongoDBMessageRetriever());
      return messageBlockingQueue;
   }

   private static int index(String topicName) {
      int hashcode = topicName.hashCode();
      hashcode = hashcode == Integer.MIN_VALUE ? 0 : Math.abs(hashcode);// 保证非负
      return hashcode % LOCK_NUM_FOR_CREATE_TOPIC_BUFFER;
   }

}
