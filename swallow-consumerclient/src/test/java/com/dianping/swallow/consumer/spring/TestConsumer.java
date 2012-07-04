package com.dianping.swallow.consumer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.consumer.ConsumerClient;
import com.dianping.swallow.consumer.MessageListener;

public class TestConsumer {

   /**
    * @param args
    */
   public static void main(String[] args) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-clientTest.xml" });
      final ConsumerClient ConsumerClient = ctx.getBean(ConsumerClient.class);
      ConsumerClient.setListener(new MessageListener() {

         @Override
         public void onMessage(SwallowMessage swallowMessage) {
            //用户得到SwallowMessage

            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
         }
      });
      ConsumerClient.beginConnect();

   }

}
