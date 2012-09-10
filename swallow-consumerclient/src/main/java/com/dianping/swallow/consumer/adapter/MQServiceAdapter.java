package com.dianping.swallow.consumer.adapter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.swallow.Destination;
import com.dianping.swallow.MQService;
import com.dianping.swallow.MessageConsumer;
import com.dianping.swallow.MessageProducer;
import com.dianping.swallow.impl.MongoMQService;

public class MQServiceAdapter implements MQService {

	private static final Logger LOG = LoggerFactory.getLogger(MQServiceAdapter.class);

	private MongoMQService oldMqService;
	private String mongoUri;

	public MQServiceAdapter(String mongoUri) {
		this.mongoUri = mongoUri;
	}

	public boolean shouldStartOldConsumer(Destination dest) {
		boolean shouldStartOldConsumer = true;
		String topicToTest = dest.getName();

		try {
			String doneTopicStr = ConfigCache.getInstance(
					EnvZooKeeperConfig.getZKAddress()).getProperty(
					"swallow.oldTopicUpdate.done");
			if (doneTopicStr != null) {
				String[] topics = doneTopicStr.trim().split(",");
				for (int i = 0; i < topics.length; i++) {
					if (topicToTest.equals(topics[i])) {
						shouldStartOldConsumer = false;
						break;
					}
				}
			}
		} catch (Exception e) {
			LOG.error("Error parsing config from lion", e);
		}

		LOG.info("start old swallow consumer " + shouldStartOldConsumer);
		return shouldStartOldConsumer;
	}

	@Override
	public MessageProducer createProducer(Destination dest,
			Map<ProducerOptionKey, Object> options) {
		throw new RuntimeException(
				"Can not support old SwallowProducer creation any more!");
	}

	@Override
	public MessageProducer createProducer(Destination dest) {
		throw new RuntimeException(
				"Can not support old SwallowProducer creation any more!");
	}

	@Override
	public synchronized MessageConsumer createConsumer(Destination dest,
			Map<ConsumerOptionKey, Object> options) {
		if (dest == null) {
			throw new IllegalArgumentException("Destination can not be null");
		}
		boolean startOldConsumer = shouldStartOldConsumer(dest);
		if (startOldConsumer && oldMqService == null) {
			oldMqService = new MongoMQService(mongoUri);
		}
		return new MessageConsumerAdapter(oldMqService, dest, options, startOldConsumer);
	}

	@Override
	public synchronized MessageConsumer createConsumer(Destination dest) {
		return createConsumer(dest, null);
	}

	@Override
	public void close() {
		if (oldMqService != null) {
			oldMqService.close();
		}
	}
}
