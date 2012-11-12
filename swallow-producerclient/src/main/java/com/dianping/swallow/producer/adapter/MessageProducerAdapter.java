package com.dianping.swallow.producer.adapter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.dianping.swallow.BinaryMessage;
import com.dianping.swallow.MQException;
import com.dianping.swallow.Message;
import com.dianping.swallow.MessageProducer;
import com.dianping.swallow.StringMessage;
import com.dianping.swallow.UndeliverableMessageHandler;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.impl.MongoBinaryMessage;
import com.dianping.swallow.impl.MongoStringMessage;
import com.dianping.swallow.producer.Producer;

public class MessageProducerAdapter implements MessageProducer {

    Producer producer;

    public MessageProducerAdapter(Producer producer) {
        this.producer = producer;
    }

    @Override
    public void send(Message msg) throws MQException {
        try {
            Map<String, String> properties = null;
            Set<String> propertyNames = msg.getPropertyNames();
            if (propertyNames != null && propertyNames.size() > 0) {
                properties = new HashMap<String, String>();
                for (String propertyName : propertyNames) {
                    properties.put(propertyName, msg.getProperty(propertyName));
                }
            }
            producer.sendMessage(msg.getContent(), properties);
        } catch (SendFailedException e) {
            throw new MQException(e);
        }
    }

    @Override
    public StringMessage createStringMessage(String content) {
        if (content == null || content.length() == 0) {
            throw new IllegalArgumentException("Can not create empty message");
        }
        return new MongoStringMessage(content);
    }

    @Override
    public BinaryMessage createBinaryMessage(byte[] content) {
        if (content == null || content.length == 0) {
            throw new IllegalArgumentException("Can not create empty message");
        }
        return new MongoBinaryMessage(content);
    }

    @Override
    public void setUndeliverableMessageHandler(UndeliverableMessageHandler handler) {

    }

}
