package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.SwallowMessage;

public class TestConsumerWithThreadPool {

   public static String       cid             = "zhangyu";
   public static String  topicName            = "xx";
   public static ConsumerType consumerType    = ConsumerType.AT_LEAST;

   public static void main(String[] args) throws Exception {
      
      final ConsumerClient cClient = new ConsumerClient(cid, topicName);
      cClient.setThreadCount(10);
      cClient.setConsumerType(consumerType);
      cClient.setListener(new MessageListener() {

         @Override
         public void onMessage(SwallowMessage swallowMessage) {
            //用户得到SwallowMessage

            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
         }
      });
      cClient.beginConnect();

   }
   
}
