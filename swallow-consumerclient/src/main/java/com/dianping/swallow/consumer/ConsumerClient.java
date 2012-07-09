package com.dianping.swallow.consumer;


public interface ConsumerClient {
   public void beginConnect();
   public void setListener(MessageListener listener);
}
