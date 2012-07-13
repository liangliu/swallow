package com.dianping.swallow.consumer.example;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerFactory;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.impl.ConsumerFactoryImpl;
import com.dianping.swallow.consumer.impl.ConsumerImpl;

public class TestConsumerWithThreadPool {

   public static String       cid          = "zhangyu";
   public static String       topicName    = "xx";
   public static ConsumerType consumerType = ConsumerType.AT_LEAST_ONCE;

   public static void main(String[] args) throws Exception {

      ConsumerFactory consumerFactory = ConsumerFactoryImpl.getInstance();
      final Consumer cClient = consumerFactory.createConsumer(Destination.topic(topicName), cid);
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
