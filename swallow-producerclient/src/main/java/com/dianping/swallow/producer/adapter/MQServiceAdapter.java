package com.dianping.swallow.producer.adapter;

import java.util.Map;

import com.dianping.swallow.Destination;
import com.dianping.swallow.MQService;
import com.dianping.swallow.MessageConsumer;
import com.dianping.swallow.MessageProducer;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

public class MQServiceAdapter implements MQService{
   
   ProducerFactory producerFactory;
   
   public MQServiceAdapter(String mongoUri) throws RemoteServiceInitFailedException{
      producerFactory = ProducerFactoryImpl.getInstance();
   }

   @Override
   public MessageProducer createProducer(Destination dest, Map<ProducerOptionKey, Object> options) {
      if(dest == null){
         throw new IllegalArgumentException("Illegal Argument!");
      }
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.ASYNC_MODE);

      if(options != null){
         try{
            int retryTimes = Integer.parseInt(options.get(ProducerOptionKey.MsgSendRetryCount).toString());
            
            if(retryTimes == -1){
               config.setAsyncRetryTimes(999999);
            } else {
               config.setAsyncRetryTimes(retryTimes);
            }
         } catch (Exception nfe){
         }
      }
      
      return new MessageProducerAdapter(producerFactory.createProducer(com.dianping.swallow.common.message.Destination.topic(dest.getName()), config));
   }

   @Override
   public MessageProducer createProducer(Destination dest) {
      return createProducer(dest, null);
   }

   @Override
   public MessageConsumer createConsumer(Destination dest, Map<ConsumerOptionKey, Object> options) {
      throw new RuntimeException("Can not support old SwallowConsumer creation any more!");
   }

   @Override
   public MessageConsumer createConsumer(Destination dest) {
      throw new RuntimeException("Can not support old SwallowConsumer creation any more!");
   }

   @Override
   public void close() {
      
   }
}
