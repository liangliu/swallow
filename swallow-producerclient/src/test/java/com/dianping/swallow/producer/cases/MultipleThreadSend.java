package com.dianping.swallow.producer.cases;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerMode;
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
         String content = "";
         for (int i = 0; i < 10; i++) {
            content += "abcdefghij1234567890!@#$%^&*()          abcdefghij1234567890!@#$%^&*()          abcdefghij1234567890";
         }
         int i = 0;
         long begin = System.currentTimeMillis();
         if (count > 0) {
            for (i = 0; i < count; i++) {
               if (i % 10000 == 0) {
                  System.out.println(pre + ": " + "sent " + i);
               }
               try {
                  producer.sendMessage(pre+ " " + ++i + ": " + content);
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
         System.out.println(">>>>>>>>>>send end. cost: " + (System.currentTimeMillis() - begin));
      }
   }

   public void syncSend(int threadNum, final int count, int freq) throws InterruptedException {
      ProducerConfig config = new ProducerConfig();
      final Producer producer = producerFactory.createProducer(Destination.topic("example"), config);
      String content = "";
      for (int i = 0; i < 10; i++) {
         content += "abcdefghij1234567890!@#$%^&*()          abcdefghij1234567890!@#$%^&*()          abcdefghij1234567890";
      }
      final String realContent = content;
      final CountDownLatch end = new CountDownLatch(threadNum);
      final CountDownLatch start = new CountDownLatch(1);
      System.out.println(">>>>>>>>>sleep a while");
      Thread.sleep(60000);

      for (int i = 0; i < threadNum;) {
         ++i;
         Thread t = new Thread(new Runnable() {
            
            @Override
            public void run() {
               try {
                  start.await();
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
               for(int i =0; i< count; i ++ ){
                  try {
                     producer.sendMessage(realContent);
                  } catch (SendFailedException e) {
                     e.printStackTrace();
                  }
               }
             end.countDown();
            }
         });
         t.start();
      }
      
    long begin = System.currentTimeMillis();
    start.countDown();
    try {
       end.await();
    } catch (InterruptedException e) {
       e.printStackTrace();
    }
    System.out.println(">>>>>>>>cost:" + (System.currentTimeMillis() - begin));
   }

   public void asyncSend(int threadNum, int threadPoolSizeForEachProducer, final int count, int freq) {
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.ASYNC_MODE);
      config.setZipped(false);
      config.setThreadPoolSize(threadPoolSizeForEachProducer);
      config.setSendMsgLeftLastSession(false);
      config.setAsyncRetryTimes(2);
      final Producer producer = producerFactory.createProducer(Destination.topic("example"), config);
      String content = "";
      for (int i = 0; i < 10; i++) {
         content += "abcdefghij1234567890!@#$%^&*()          abcdefghij1234567890!@#$%^&*()          abcdefghij1234567890";
      }
      final String realContent = content;
      final CountDownLatch end = new CountDownLatch(threadNum);
      final CountDownLatch start = new CountDownLatch(1);

      for (int i = 0; i < threadNum;) {
         ++i;
         Thread t = new Thread(new Runnable() {
            
            @Override
            public void run() {
               try {
                  start.await();
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
               for(int i =0; i< count; i ++ ){
                  try {
                     producer.sendMessage(realContent);
                  } catch (SendFailedException e) {
                     e.printStackTrace();
                  }
               }
             end.countDown();
            }
         });
         t.start();
      }
      
    long begin = System.currentTimeMillis();
    start.countDown();
    try {
       end.await();
    } catch (InterruptedException e) {
       e.printStackTrace();
    }
    System.out.println(">>>>>>>>cost:" + (System.currentTimeMillis() - begin));
    

   }

   public static void main(String[] args) throws RemoteServiceInitFailedException, FileQueueClosedException, InterruptedException {

      new MultipleThreadSend().asyncSend(20, 2, 100000, -1);
   }
}
