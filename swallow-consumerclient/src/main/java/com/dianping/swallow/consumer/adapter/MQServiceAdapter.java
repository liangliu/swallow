package com.dianping.swallow.consumer.adapter;

import java.util.Map;

import com.dianping.swallow.Destination;
import com.dianping.swallow.MQService;
import com.dianping.swallow.MessageConsumer;
import com.dianping.swallow.MessageProducer;
import com.dianping.swallow.impl.MongoMQService;

public class MQServiceAdapter implements MQService{
	
	private MongoMQService oldMqService;
	private String mongoUri;
	
	public MQServiceAdapter(String mongoUri) {
		this.mongoUri = mongoUri;
	}
	
	public static boolean shouldStartOldConsumer(Destination dest) {
		return true;
	}

   @Override
   public MessageProducer createProducer(Destination dest, Map<ProducerOptionKey, Object> options) {
	   throw new RuntimeException("Can not support old SwallowProducer creation any more!");
   }

   @Override
   public MessageProducer createProducer(Destination dest) {
	   throw new RuntimeException("Can not support old SwallowProducer creation any more!");
   }

   @Override
   public synchronized MessageConsumer createConsumer(Destination dest, Map<ConsumerOptionKey, Object> options) {
	   if(shouldStartOldConsumer(dest) && oldMqService == null) {
		   oldMqService = new MongoMQService(mongoUri);
	   }
      return new MessageConsumerAdapter(oldMqService, dest, options);
   }

   @Override
   public synchronized MessageConsumer createConsumer(Destination dest) {
	   return createConsumer(dest, null);
   }

   @Override
   public void close() {
	   oldMqService.close();
   }

}
