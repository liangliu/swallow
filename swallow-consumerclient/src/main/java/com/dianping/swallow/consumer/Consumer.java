package com.dianping.swallow.consumer;

public interface Consumer {
   /**
    * consumerClient开始工作
    */
   void start();

   /**
    * 设置listener，用于回调
    * 
    * @param listener
    */
   void setListener(MessageListener listener);

   /**
    * 发送关闭channel的信号
    */
   void close();
}
