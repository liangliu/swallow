package com.dianping.swallow.dashboard.consumer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.consumer.Consumer;

public class TestConsumer {

   /**
    * @param args
    * @throws InterruptedException 
    */
   public static void main(String[] args) throws InterruptedException {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-consumer.xml" });
      final Consumer consumerClient = (Consumer) ctx.getBean("consumerClient");  
      consumerClient.start();
   }

}
