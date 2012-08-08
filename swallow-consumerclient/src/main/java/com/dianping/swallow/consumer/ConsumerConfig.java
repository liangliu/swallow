package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.consumer.MessageFilter;

/**
 * 该类用于设置Consumer的选项，每个Consumer对应一个ConsumerConfig
 * 
 * @author kezhu.wu
 */
public class ConsumerConfig {

   private int           threadPoolSize                           = 1;
   private MessageFilter messageFilter                            = MessageFilter.AllMatchFilter;
   private ConsumerType  consumerType                             = ConsumerType.DURABLE_AT_LEAST_ONCE;
   /**
    * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
    * 次重试之间最小的停顿时间
    */
   private int           delayBaseOnBackoutMessageException       = 100;                               //ms
   /**
    * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
    * 次重试之间最大的停顿时间
    */
   private int           delayUpperboundOnBackoutMessageException = 3000;                              //ms
   /** 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，最多重试的次数 */
   private int           retryCountOnBackoutMessageException      = 5;                                 //重试次数

   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * 设置consumer处理消息的线程池线程数，默认为1
    * 
    * @param threadPoolSize
    */
   public void setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
   }

   /**
    * 返回消息过滤方式
    */
   public MessageFilter getMessageFilter() {
      return messageFilter;
   }

   /**
    * 设置消息过滤方式
    * 
    * @param messageFilter
    */
   public void setMessageFilter(MessageFilter messageFilter) {
      this.messageFilter = messageFilter;
   }

   /**
    * Consumer的类型，包括3种类型：<br>
    * 1.AT_MOST：尽量保证消息最多消费一次，不出现重复消费（注意：只是尽量保证，而非绝对保证。）<br>
    * 2.AT_LEAST：尽量保证消息最少消费一次，不出现消息丢失的情况（注意：只是尽量保证，而非绝对保证。）<br>
    * 3.NON_DURABLE：临时的消费类型，从当前的消息开始消费，不会对消费状态进行持久化，Server重启后将重新开始。
    */
   public ConsumerType getConsumerType() {
      return consumerType;
   }

   /**
    * Consumer的类型，包括3种类型：<br>
    * 1.AT_MOST：尽量保证消息最多消费一次，不出现重复消费（注意：只是尽量保证，而非绝对保证。）<br>
    * 2.AT_LEAST：尽量保证消息最少消费一次，不出现消息丢失的情况（注意：只是尽量保证，而非绝对保证。）<br>
    * 3.NON_DURABLE：临时的消费类型，从当前的消息开始消费，不会对消费状态进行持久化，Server重启后将重新开始。
    */
   public void setConsumerType(ConsumerType consumerType) {
      this.consumerType = consumerType;
   }

   /**
    * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
    * 次重试之间最小的停顿时间 *
    * <p>
    * 默认值为100
    * </p>
    */
   public int getDelayBaseOnBackoutMessageException() {
      return delayBaseOnBackoutMessageException;
   }

   /**
    * 当MessageListener.onMessage(Message)抛出BackoutMessageException异常时，2
    * 次重试之间最小的停顿时间 *
    * <p>
    * 默认值为100
    * </p>
    */
   public void setDelayBaseOnBackoutMessageException(int delayBaseOnBackoutMessageException) {
      this.delayBaseOnBackoutMessageException = delayBaseOnBackoutMessageException;
   }

   /**
    * delayUpperboundOnBackoutMessageException表示“当MessageListener.onMessage(
    * Message)抛出BackoutMessageException异常时，2次重试之间最大的停顿时间” *
    * <p>
    * 默认值为3000
    * </p>
    */
   public int getDelayUpperboundOnBackoutMessageException() {
      return delayUpperboundOnBackoutMessageException;
   }

   /**
    * delayUpperboundOnBackoutMessageException表示“当MessageListener.onMessage(
    * Message)抛出BackoutMessageException异常时，2次重试之间最大的停顿时间” *
    * <p>
    * 默认值为3000
    * </p>
    */
   public void setDelayUpperboundOnBackoutMessageException(int delayUpperboundOnBackoutMessageException) {
      this.delayUpperboundOnBackoutMessageException = delayUpperboundOnBackoutMessageException;
   }

   /**
    * retryCountOnBackoutMessageException表示“当MessageListener.onMessage(Message)
    * 抛出BackoutMessageException异常时，最多重试的次数” *
    * <p>
    * 默认值为5
    * </p>
    */
   public int getRetryCountOnBackoutMessageException() {
      return retryCountOnBackoutMessageException;
   }

   /**
    * retryCountOnBackoutMessageException表示“当MessageListener.onMessage(Message)
    * 抛出BackoutMessageException异常时，最多重试的次数”<br>
    * <p>
    * 默认值为5
    * </p>
    */
   public void setRetryCountOnBackoutMessageException(int retryCountOnBackoutMessageException) {
      this.retryCountOnBackoutMessageException = retryCountOnBackoutMessageException;
   }

}
