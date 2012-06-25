package com.dianping.swallow.impl;

import java.util.Map;

import com.dianping.swallow.Destination;
import com.dianping.swallow.MQService;
import com.dianping.swallow.MessageConsumer;
import com.dianping.swallow.MessageProducer;

public class MongoMQService implements MQService {
	
	public MongoMQService(String mongoUri) {
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public MessageConsumer createConsumer(Destination dest, Map<ConsumerOptionKey, Object> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageConsumer createConsumer(Destination dest) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageProducer createProducer(Destination dest, Map<ProducerOptionKey, Object> options) {
		return new MessageProducerAdapter(dest, options);
	}

	@Override
	public MessageProducer createProducer(Destination dest) {
		// TODO Auto-generated method stub
		return null;
	}

}
