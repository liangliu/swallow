package com.dianping.swallow.example.consumer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.MessageListener;

public class TestNonDurableConsumer {

   /**
    * @param args
    */
   public static void main(String[] args) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-consumer.xml" });
      final Consumer ConsumerClient = (Consumer) ctx.getBean("nonDurableConsumerClient");
      ConsumerClient.setListener(new MessageListener() {

         @Override
         public void onMessage(Message swallowMessage) {
            //用户得到SwallowMessage

            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
            try {
               Thread.sleep(100);
            } catch (InterruptedException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
            }
         }
      });
      ConsumerClient.start();

   }

}
