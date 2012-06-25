package com.dianping.swallow.impl;

import java.util.Map;

import com.dianping.swallow.BinaryMessage;
import com.dianping.swallow.Destination;
import com.dianping.swallow.MQException;
import com.dianping.swallow.Message;
import com.dianping.swallow.MessageProducer;
import com.dianping.swallow.StringMessage;
import com.dianping.swallow.UndeliverableMessageHandler;
import com.dianping.swallow.MQService.ProducerOptionKey;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.impl.ProducerImpl;

public class MessageProducerAdapter implements MessageProducer {
	
	Producer targetP;

	public MessageProducerAdapter(Destination dest, Map<ProducerOptionKey, Object> options) {
		try {
			targetP = ProducerImpl.getInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public BinaryMessage createBinaryMessage(byte[] content) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringMessage createStringMessage(String content) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void send(Message msg) throws MQException {
		
	}

	@Override
	public void setUndeliverableMessageHandler(UndeliverableMessageHandler handler) {
		// TODO Auto-generated method stub

	}

}
