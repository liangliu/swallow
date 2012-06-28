package com.dianping.swallow.consumerserver.worker;

import java.io.IOException;
import java.util.Map;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

public class ConsumerWorkerManager implements ConsumerWorker {
	
	private Map<ConsumerId, ConsumerWorker> consumerId2ConsumerWorker = new ConcurrentHashMap<ConsumerId, ConsumerWorker>();

	@Override
	public void handleGreet(Channel channel, ConsumerInfo consumerInfo) {
		findOrCreateConsumerWorker(consumerInfo).handleGreet(channel, consumerInfo);
	}
	
	@Override
	public void handleAck(Channel channel, ConsumerInfo consumerInfo, Long ackedMsgId) {
		findOrCreateConsumerWorker(consumerInfo).handleAck(channel, consumerInfo, ackedMsgId);
	}
	
	@Override
	public void handleChannelDisconnect(Channel channel, ConsumerInfo consumerInfo) {
		findOrCreateConsumerWorker(consumerInfo).handleChannelDisconnect(channel, consumerInfo);
	}

	@Override
	public void close() throws IOException {
		for (Map.Entry<ConsumerId, ConsumerWorker> entry : consumerId2ConsumerWorker.entrySet()) {
			entry.getValue().close();
		}
	}
	
	private ConsumerWorker findOrCreateConsumerWorker(ConsumerInfo consumerInfo) {
		ConsumerId consumerId = consumerInfo.getConsumerId();
		ConsumerWorker consumerWorker = consumerId2ConsumerWorker.get(consumerId);
		if(consumerWorker == null) {
			synchronized (this) {
				if(consumerWorker == null) {
					consumerWorker = new ConsumerWorkerImpl(consumerInfo);
					consumerId2ConsumerWorker.put(consumerId, consumerWorker);
				}
			}
		}
		return consumerWorker;
	}
	
}
