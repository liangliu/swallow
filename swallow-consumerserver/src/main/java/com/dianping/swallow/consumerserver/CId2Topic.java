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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((consumerId == null) ? 0 : consumerId.hashCode());
		result = prime * result
				+ ((topicName == null) ? 0 : topicName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CId2Topic other = (CId2Topic) obj;
		if (consumerId == null) {
			if (other.consumerId != null)
				return false;
		} else if (!consumerId.equals(other.consumerId))
			return false;
		if (topicName == null) {
			if (other.topicName != null)
				return false;
		} else if (!topicName.equals(other.topicName))
			return false;
		return true;
	}
	
}
