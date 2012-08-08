package com.dianping.swallow.consumer;

import com.dianping.swallow.common.message.Message;

public interface MessageListener {

   /**
    * 消息处理回调方法
    * 
    * @param msg
    * @throws BackoutMessageException
    *            当consumer无法处理Message且希望重试该消息时，可以抛出该异常，Swallow接收到该异常会使用重试策略（见
    *            {@link com.dianping.swallow.consumer.ConsumerConfig} 的
    *            delayBaseOnBackoutMessageException
    *            ，delayUpperboundOnBackoutMessageException
    *            ，retryCountOnBackoutMessageException选项）进行重试，即
    *            {@link com.dianping.swallow.consumer.MessageListener}
    *            的onMessage方法会被重复调用
    */
   void onMessage(Message msg) throws BackoutMessageException;

}
