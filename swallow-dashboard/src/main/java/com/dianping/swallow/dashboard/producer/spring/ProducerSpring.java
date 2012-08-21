package com.dianping.swallow.dashboard.producer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.Producer;

public class ProducerSpring {
   public static void main(String[] args) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(
            new String[] { "applicationContext-producer.xml" });
      Producer producer = (Producer) ctx.getBean("producerClient");
      try {
         System.out.println(producer.sendMessage("Hello world."));
      } catch (SendFailedException e) {
         e.printStackTrace();
      }
   }
}
