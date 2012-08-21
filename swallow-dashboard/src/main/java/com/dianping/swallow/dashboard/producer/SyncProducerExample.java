package com.dianping.swallow.dashboard.producer;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

public class SyncProducerExample {

   public static void main(String[] args) throws Exception {
      ProducerConfig config = new ProducerConfig();
      Producer p = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("example"), config );
      for (int i = 0; i < Integer.MAX_VALUE; i++) {
         p.sendMessage("消息-" + i);
         Thread.sleep(2000);
      }
   }
   
}
