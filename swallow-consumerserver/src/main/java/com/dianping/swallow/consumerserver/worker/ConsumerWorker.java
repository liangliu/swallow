package com.dianping.swallow.consumerserver.worker;

import java.io.Closeable;

import org.jboss.netty.channel.Channel;

public interface ConsumerWorker extends Closeable {

	public void handleGreet(Channel channel, ConsumerInfo consumerInfo);

	public void handleAck(Channel channel, ConsumerInfo consumerInfo, Long ackedMsgId);

	public void handleChannelDisconnect(Channel channel, ConsumerInfo consumerInfo);

}
