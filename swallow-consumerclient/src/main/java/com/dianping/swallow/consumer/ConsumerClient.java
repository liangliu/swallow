package com.dianping.swallow.consumer;


public interface ConsumerClient {
   /**
    * consumerClient开始工作
    */
   public void start();
   /**
    * 设置listener，用于回调
    * @param listener
    */
   public void setListener(MessageListener listener);
}
