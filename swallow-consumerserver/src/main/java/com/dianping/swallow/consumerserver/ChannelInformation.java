package com.dianping.swallow.consumerserver;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;

public class ChannelInformation {

	private Destination dest;
	
	private String consumerId;
	
	private ConsumerType consumerType;

	public Destination getDest() {
		return dest;
	}

	public void setDest(Destination dest) {
		this.dest = dest;
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public ConsumerType getConsumerType() {
		return consumerType;
	}

	public void setConsumerType(ConsumerType consumerType) {
		this.consumerType = consumerType;
	}

	public ChannelInformation(Destination dest, String consumerId, ConsumerType consumerType) {
		this.dest = dest;
		this.consumerId = consumerId;
		this.consumerType = consumerType;
	}
	
	
}
