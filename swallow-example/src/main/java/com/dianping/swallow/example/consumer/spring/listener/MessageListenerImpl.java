package com.dianping.swallow.example.consumer.spring.listener;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.MessageListener;

public class MessageListenerImpl implements MessageListener {

   @Override
   public void onMessage(Message swallowMessage) {
      
      System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent()+ ":" + swallowMessage.getType());
      try {
         Thread.sleep(500);
      } catch (InterruptedException e) {

         e.printStackTrace();
      }
   }

}
