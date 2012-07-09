package com.dianping.swallow.consumer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.impl.ConsumerClientImpl;

public class TestNonDurableConsumer {

   /**
    * @param args
    */
   public static void main(String[] args) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-cClientTest.xml" });
      final ConsumerClientImpl ConsumerClient = (ConsumerClientImpl) ctx.getBean("nonDurableConsumerClient");
      ConsumerClient.setListener(new MessageListener() {

         @Override
         public void onMessage(Message swallowMessage) {
            //用户得到SwallowMessage

            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
         }
      });
      ConsumerClient.beginConnect();

   }

}
