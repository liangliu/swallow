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
    * @throws InterruptedException 
    */
   public static void main(String[] args) throws InterruptedException {
      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-consumerclientTest.xml" });
      final ConsumerClient consumerClient = (ConsumerClientImpl) ctx.getBean("consumerClient");  
      consumerClient.setListener(new MessageListener() {
         int i = 0;
         @Override
         public void onMessage(Message swallowMessage) {
            
            //用户得到SwallowMessage
            i++;
            if(i==500){
               ((ConsumerClientImpl)consumerClient).setNeedClose(Boolean.TRUE);
            }
            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent()+ ":" + swallowMessage.getType());
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
            }
         }
      });
      consumerClient.start();
      
   }

}
