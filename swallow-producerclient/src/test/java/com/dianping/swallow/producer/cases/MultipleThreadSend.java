package com.dianping.swallow.producer.cases;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

public class MultipleThreadSend {
   private Logger                     logger = LoggerFactory.getLogger(MultipleThreadSend.class);
   private static ProducerFactoryImpl producerFactory;

   public MultipleThreadSend() throws RemoteServiceInitFailedException {
      producerFactory = ProducerFactoryImpl.getInstance();
   }

   class TskSync implements Runnable {
      Producer producer;
      int      count;
      int      freq;
      String   pre;

      public TskSync(Producer producer, int count, int freq, String pre) {
         this.producer = producer;
         this.count = count;
         this.freq = freq;
         this.pre = pre;
      }

      @Override
      public void run() {
         int i = 0;
         if (count > 0) {
            for (i = 0; i < count; i++) {
               try {
                  System.out.println(producer.sendMessage(pre + ": " + ++i));
                  Thread.sleep(freq < 0 ? 0 : freq);
               } catch (SendFailedException e) {
                  logger.warn("Message sent failed, message ID=" + i);
               } catch (InterruptedException e) {
                  logger.warn("Sleep interrupted.");
                  System.exit(0);
               }
            }
         } else {
            while (true) {
               try {
                  System.out.println(producer.sendMessage(pre + ": " + ++i));
                  Thread.sleep(freq < 0 ? 0 : freq);
               } catch (SendFailedException e) {
                  logger.warn("Message sent failed, message ID=" + i, e);
                  System.exit(0);
               } catch (InterruptedException e) {
                  logger.warn("Sleep interrupted.");
                  System.exit(0);
               }
            }
         }
      }
   }

   public void syncSend(int threadNum, int count, int freq) {
      ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
      ProducerConfig config = new ProducerConfig();
      config.setRetryTimes(1);
      Producer producer = producerFactory.createProducer(Destination.topic("example"), config);
      for (int i = 0; i < threadNum;) {
         ++i;
         threadPool.execute(new TskSync(producer, count, freq, "Thread " + i + ": "));
      }
   }

   public static void main(String[] args) throws RemoteServiceInitFailedException {
      new MultipleThreadSend().syncSend(500, -1, -1);
   }
}
