package com.dianping.swallow.consumerserver.worker;

import com.dianping.swallow.common.consumer.ConsumerType;

public class ConsumerInfo {

	private ConsumerId consumerId;
	private ConsumerType consumerType;

	public ConsumerInfo(ConsumerId consumerId, ConsumerType consumerType) {
		super();
		this.consumerId = consumerId;
		this.consumerType = consumerType;
	}

	public ConsumerId getConsumerId() {
		return consumerId;
	}

	public ConsumerType getConsumerType() {
		return consumerType;
	}
	public String toString() {
		return consumerId.getDest().getName() + consumerId.getConsumerId();
	}
}
