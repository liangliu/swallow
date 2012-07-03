package com.dianping.swallow.consumerserver.worker;

import java.io.Closeable;

import org.jboss.netty.channel.Channel;

import com.dianping.swallow.common.consumer.ACKHandlerType;

public interface ConsumerWorker extends Closeable {

   public void handleGreet(Channel channel, int clientThreadCount);

   public void handleAck(Channel channel, Long ackedMsgId, ACKHandlerType type);

   public void handleChannelDisconnect(Channel channel);

   public void sendMessageByPollFreeChannelQueue();
}
