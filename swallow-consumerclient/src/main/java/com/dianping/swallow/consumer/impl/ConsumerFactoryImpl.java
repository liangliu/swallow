package com.dianping.swallow.consumer.impl;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerConfig;
import com.dianping.swallow.consumer.ConsumerFactory;

public class ConsumerFactoryImpl implements ConsumerFactory {
   
   private static ConsumerFactoryImpl instance = new ConsumerFactoryImpl();
   
   private ConsumerFactoryImpl() {
      //TODO 读取lion或本地配置，变成map，topicName trim()一下
   }
   
   public static ConsumerFactory getInstance() {
      return instance;
   }

   @Override
   public Consumer createConsumer(Destination dest, String consumerId, ConsumerConfig config) {
      // TODO 将dest对应的master/slave地址传给构造函数
      return new ConsumerImpl(dest, consumerId, config);
   }

   @Override
   public Consumer createConsumer(Destination dest, String consumerId) {
      return new ConsumerImpl(dest, consumerId, new ConsumerConfig());
   }

   @Override
   public Consumer createConsumer(Destination dest, ConsumerConfig config) {
      return new ConsumerImpl(dest, config);
   }

   @Override
   public Consumer createConsumer(Destination dest) {
      return new ConsumerImpl(dest, new ConsumerConfig());
   }

}
