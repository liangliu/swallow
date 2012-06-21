package com.dianping.swallow.producerserver;

import com.dianping.swallow.common.message.SwallowMessage;

public interface TopicDAO {
	boolean saveMessage(String topicName, SwallowMessage msg);
}
