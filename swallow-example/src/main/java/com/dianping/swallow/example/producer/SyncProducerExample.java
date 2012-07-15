package com.dianping.swallow.example.producer;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

public class SyncProducerExample {

   public static void main(String[] args) throws Exception, RemoteServiceDownException, NullContentException, FileQueueClosedException {
      ProducerConfig config = new ProducerConfig();
      Producer p = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("example"), config );
      for (int i = 0; i < 10; i++) {
         p.sendMessage("" + i);
      }
   }
   
}
