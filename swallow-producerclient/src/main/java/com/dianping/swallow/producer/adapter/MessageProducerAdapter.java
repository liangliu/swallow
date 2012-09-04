package com.dianping.swallow.producer.adapter;

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
         producer.sendMessage(msg.getContent());
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
