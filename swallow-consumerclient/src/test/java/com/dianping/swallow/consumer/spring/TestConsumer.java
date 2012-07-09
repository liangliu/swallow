package com.dianping.swallow.consumer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.ConsumerClient;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.impl.ConsumerClientImpl;

public class TestConsumer {

   /**
    * @param args
    */
   public static void main(String[] args) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-consumerclientTest.xml" });
      final ConsumerClient consumerClient = (ConsumerClientImpl) ctx.getBean("consumerClient");
      consumerClient.setListener(new MessageListener() {

         @Override
         public void onMessage(Message swallowMessage) {
            //用户得到SwallowMessage

            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
         }
      });
      consumerClient.beginConnect();

   }

}
