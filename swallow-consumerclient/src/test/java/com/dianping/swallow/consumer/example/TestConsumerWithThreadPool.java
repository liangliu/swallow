package com.dianping.swallow.consumer.example;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.ConsumerClient;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.impl.ConsumerClientImpl;

public class TestConsumerWithThreadPool {

   public static String       cid          = "zhangyu";
   public static String       topicName    = "xx";
   public static ConsumerType consumerType = ConsumerType.AT_LEAST;

   public static void main(String[] args) throws Exception {

      final ConsumerClient cClient = new ConsumerClientImpl(cid, topicName);
      cClient.setListener(new MessageListener() {

         @Override
         public void onMessage(Message swallowMessage) {
            //用户得到SwallowMessage

            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
         }
      });
      cClient.start();

   }

}
