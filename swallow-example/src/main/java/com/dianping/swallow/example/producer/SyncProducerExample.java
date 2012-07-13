package com.dianping.swallow.example.producer;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

public class SyncProducerExample {

   public static void main(String[] args) {
      ProducerConfig config = new ProducerConfig();
      Producer p = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("example"), config );
   }
   
}
