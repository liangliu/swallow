package com.dianping.swallow.producerserver.impl;

import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.producerserver.TopicDAO;

public class TopicDAOImpl implements TopicDAO{

	@Override
	public void saveMessage(String topicName, SwallowMessage msg) {
		// TODO Auto-generated method stub
		System.out.println(msg.toString());
	}

}
