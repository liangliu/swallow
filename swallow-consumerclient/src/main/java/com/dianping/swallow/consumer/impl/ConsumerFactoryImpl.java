package com.dianping.swallow.consumer.impl;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerConfig;
import com.dianping.swallow.consumer.ConsumerFactory;

public class ConsumerFactoryImpl implements ConsumerFactory {
   
   private static ConsumerFactoryImpl instance = new ConsumerFactoryImpl();
   
   private ConsumerFactoryImpl() {
   }
   
   public static ConsumerFactory getInstance() {
      return instance;
   }

   @Override
   public Consumer createConsumer(Destination dest, String consumerId, ConsumerConfig config) {
      return new ConsumerImpl(dest, consumerId, config);
   }

   @Override
   public Consumer createConsumer(Destination dest, String consumerId) {
      return new ConsumerImpl(dest, consumerId, new ConsumerConfig());

   }

}
