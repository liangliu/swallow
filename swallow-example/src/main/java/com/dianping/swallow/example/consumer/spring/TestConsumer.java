package com.dianping.swallow.example.consumer.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.impl.ConsumerImpl;

public class TestConsumer {

   /**
    * @param args
    * @throws InterruptedException 
    */
   public static void main(String[] args) throws InterruptedException {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-consumer.xml" });
      final Consumer consumerClient = (Consumer) ctx.getBean("consumerClient");  
      consumerClient.setListener(new MessageListener() {
         int i = 0;
         @Override
         public void onMessage(Message swallowMessage) {
            
            //用于测试客户端传关闭命令道服务器端
            i++;
            if(i==500){
               ((ConsumerImpl)consumerClient).close();
            }
            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent()+ ":" + swallowMessage.getType());
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {

               e.printStackTrace();
            }
         }
      });
      consumerClient.start();
      
   }

}
