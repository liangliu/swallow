package com.dianping.swallow.consumerserver.worker;

import org.jboss.netty.channel.Channel;

import com.dianping.swallow.common.internal.consumer.ACKHandlerType;

public interface ConsumerWorker{
   /**
    * 处理greet信息
    * @param channel
    * @param clientThreadCount
    */
   public void handleGreet(Channel channel, int clientThreadCount);
   /**
    * 处理ack信息
    * @param channel
    * @param ackedMsgId
    * @param type
    */
   public void handleAck(Channel channel, Long ackedMsgId, ACKHandlerType type);
   /**
    * channel断开时所做的操作
    * @param channel
    */
   public void handleChannelDisconnect(Channel channel);
   /**
    * 关闭获取消息的线程
    */
   public void closeMessageFetcherThread();
   /**
    * 关闭处理ack的线程
    */
   public void closeAckExecutor();
   /**
    * 判断同consumerId下的所有的连接是否都不存在
    * @return
    */
   public boolean allChannelDisconnected();
}
