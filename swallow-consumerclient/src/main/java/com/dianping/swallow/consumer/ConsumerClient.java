package com.dianping.swallow.consumer;


public interface ConsumerClient {
   public void start();
   public void setListener(MessageListener listener);
}
