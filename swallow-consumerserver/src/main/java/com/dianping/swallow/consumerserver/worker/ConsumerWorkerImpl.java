package com.dianping.swallow.consumerserver.worker;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.jboss.netty.channel.Channel;

public class ConsumerWorkerImpl implements ConsumerWorker {
	
	private ConsumerInfo consumerInfo;
	private BlockingQueue<Channel> readyChannels;

	public ConsumerWorkerImpl(ConsumerInfo consumerInfo) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handleAck(Channel channel, ConsumerInfo consumerInfo, Long ackedMsgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleChannelDisconnect(Channel channel, ConsumerInfo consumerInfo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleGreet(Channel channel, ConsumerInfo consumerInfo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
