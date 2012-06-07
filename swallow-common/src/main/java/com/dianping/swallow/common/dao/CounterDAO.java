package com.dianping.swallow.common.dao;

import org.bson.types.BSONTimestamp;

public interface CounterDAO {
	
	BSONTimestamp getMaxTimeStamp(String topicId, String consumerId);
	
	int addCounter(String topicId, String consumerId);
}
