package com.dianping.swallow.consumerserver.worker;

import java.io.Closeable;

import org.jboss.netty.channel.Channel;

public interface ConsumerWorker extends Closeable {

	public void handleGreet(Channel channel);

	public void handleAck(Channel channel, Long ackedMsgId);

	public void handleChannelDisconnect(Channel channel);

	public void sendMessageByPollFreeChannelQueue();
}
