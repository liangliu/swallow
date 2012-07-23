package com.dianping.swallow.producer.cases;

import java.util.Map;

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

   public static void syncSendSome(String content, int count, int freq, Map<String, String> properties, String type)
         throws Exception {
      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("songtong"));
      int sent = 0;
      if (count <= 0) {
         while (true) {
            System.out.println(producer.sendMessage(++sent + ": " + content, properties, type));
            Thread.sleep(freq < 0 ? 0 : freq);
         }
      } else {
         for (int i = 0; i < count; i++) {
            System.out.println(producer.sendMessage(++sent + ": " + content, properties, type));
            Thread.sleep(freq < 0 ? 0 : freq);
         }
      }
   }

   public static void syncSendSomeObjectDemoWithZipped(int count, int freq) throws Exception {
      ProducerConfig config = new ProducerConfig();
      config.setRetryTimes(1);
      config.setZipped(true);

      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("example"), config);

      DemoObject demoObj = new DemoObject();
      if (count <= 0) {
         System.out.println(producer.sendMessage(demoObj));
         Thread.sleep(freq < 0 ? 0 : freq);
      } else {
         for (int i = 0; i < count; i++) {
            demoObj.setA(i);
            System.out.println(producer.sendMessage(demoObj));
            Thread.sleep(freq < 0 ? 0 : freq);
         }
      }
   }

   public static void asyncSendSome(String content, int count, int freq, Map<String, String> properties, String type)
         throws Exception {
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(1);
      config.setSendMsgLeftLastSession(false);
      config.setThreadPoolSize(2);
      config.setZipped(false);

      Producer producer = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("songtong"), config);
      long begin = System.currentTimeMillis();
      int sent = 0;
      if (count <= 0) {
         while (true) {
            System.out.println(producer.sendMessage(++sent + ": " + content, properties, type));
            Thread.sleep(freq < 0 ? 0 : freq);
         }
      } else {
         for (int i = 0; i < count; i++) {
            System.out.println(producer.sendMessage(++sent + ": " + content, properties, type));
            Thread.sleep(freq < 0 ? 0 : freq);
         }
         System.out.println("total cost: " + (System.currentTimeMillis() - begin));
      }
   }

   public static void main(String[] args) throws Exception {
//      SingleThreadSend.syncSendSome("Hello world", 1000, 1000, null, "songtong");
      //      SingleThreadSend.syncSendSomeObjectDemoWithZipped(1000, 1000);
            SingleThreadSend.asyncSendSome("Hello orange", 1000, 100, null, null);
   }
}
