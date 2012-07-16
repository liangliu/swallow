package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.consumer.MessageFilter;

public class ConsumerConfig {

   private int           threadPoolSize = 1;
   private MessageFilter messageFilter  = MessageFilter.AllMatchFilter;
   private ConsumerType        consumerType                 = ConsumerType.AT_MOST_ONCE;

   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * 设置consumer处理消息的线程池线程数，默认为1
    * @param threadPoolSize
    */
   public void setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
   }

   public MessageFilter getMessageFilter() {
      return messageFilter;
   }

   /**
    * 设置消息过滤方式
    * @param messageFilter
    */
   public void setMessageFilter(MessageFilter messageFilter) {
      this.messageFilter = messageFilter;
   }

   public ConsumerType getConsumerType() {
      return consumerType;
   }

   public void setConsumerType(ConsumerType consumerType) {
      this.consumerType = consumerType;
   }

}
