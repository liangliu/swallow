package com.dianping.swallow.consumerserver.worker;

import org.jboss.netty.channel.Channel;

import com.dianping.swallow.common.internal.consumer.ACKHandlerType;
import com.dianping.swallow.common.message.Destination;

public interface ConsumerWorker{
   /**
    * 处理greet信息
    * @param channel 
    * @param clientThreadCount consumer处理消息的线程池线程数
    */
   public void handleGreet(Channel channel, int clientThreadCount);
   /**
    * 处理ack信息
    * @param channel
    * @param ackedMsgId 客户端返回的messageId
    * @param type 接收到ack后的处理类型类型为{@link ACKHandlerType}
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
