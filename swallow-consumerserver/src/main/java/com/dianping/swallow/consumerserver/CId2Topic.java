package com.dianping.swallow.consumerserver;

public class CId2Topic {

	private String consumerId;
	
	private String topicName;

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public CId2Topic(String consumerId, String topicName) {
		super();
		this.consumerId = consumerId;
		this.topicName = topicName;
	}
	
	public String toString() {
        return topicName + consumerId;
   }
}
