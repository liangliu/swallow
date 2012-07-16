package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.MessageFilter;

public class ConsumerConfig {

   private int           threadPoolSize = 1;
   private MessageFilter messageFilter  = MessageFilter.AllMatchFilter;

   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   public void setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
   }

   public MessageFilter getMessageFilter() {
      return messageFilter;
   }

   public void setMessageFilter(MessageFilter messageFilter) {
      this.messageFilter = messageFilter;
   }

}
