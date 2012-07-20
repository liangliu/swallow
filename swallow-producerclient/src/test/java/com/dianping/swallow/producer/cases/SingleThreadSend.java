package com.dianping.swallow.producer.cases;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

@SuppressWarnings("unused")
class DemoObject {
   private int        a  = 1000;
   private float      b  = 10.123f;
   private String     c  = "hello world";
   private TempObject to = new TempObject();

   public int getA() {
      return a;
   }

   public void setA(int a) {
      this.a = a;
   }
}

class TempObject {
   @SuppressWarnings("unused")
   private String d = "hello kitty";
}

public class SingleThreadSend {

   public static void syncSendSome(Object content, int count, int freq) throws Exception {
      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("songtong"));
      for (int i = 0; i < count; i++) {
         System.out.println(producer.sendMessage(content));
         Thread.sleep(freq < 0 ? 0 : freq);
      }
   }

   public static void syncSendAlways(Object content, int freq) throws Exception {
      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("songtong"));
      while (true) {
         System.out.println(producer.sendMessage(content));
         Thread.sleep(freq < 0 ? 0 : freq);
      }
   }

   public static void asyncSendSome(Object content, int count, int freq) throws Exception {
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(1);
      config.setSendMsgLeftLastSession(false);
      config.setThreadPoolSize(2);
      config.setZipped(false);

      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("songtong"), config);
      long begin = System.currentTimeMillis();
      for (int i = 0; i < count; i++) {
         producer.sendMessage(content);
      }
      System.out.println("total cost: " + (System.currentTimeMillis() - begin));
   }

   public static void main(String[] args) throws Exception {
      ProducerConfig config = new ProducerConfig();
      config.setRetryTimes(1);
      config.setZipped(true);

      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("songtong"), config);

      DemoObject demoObj = new DemoObject();

      for (int i = 0; i < 100; i++) {
         demoObj.setA(i);
         System.out.println(producer.sendMessage(demoObj));
         Thread.sleep(100);
      }
   }
}
