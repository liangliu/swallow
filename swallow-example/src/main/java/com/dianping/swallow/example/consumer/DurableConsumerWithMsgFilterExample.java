package com.dianping.swallow.example.consumer;

import java.util.HashSet;
import java.util.Set;

import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerConfig;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.impl.ConsumerFactoryImpl;

/**
 * @rundemo_name 消费者例子(持久并带过滤功能)
 */
public class DurableConsumerWithMsgFilterExample {

   public static void main(String[] args) {
      ConsumerConfig config = new ConsumerConfig();
      //以下两项根据自己情况而定，默认是不需要配的
      config.setThreadPoolSize(10);
      Set<String> matchedTypes = new HashSet<String>();
      matchedTypes.add("myType");
      config.setMessageFilter(MessageFilter.createInSetMessageFilter(matchedTypes));

      Consumer c = ConsumerFactoryImpl.getInstance().createConsumer(Destination.topic("example"), "myIdWithFilter",
            config);
      c.setListener(new MessageListener() {

         @Override
         public void onMessage(Message msg) {
            System.out.println(msg.getContent());
            //            System.out.println(msg.transferContentToBean(MsgClass.class));
         }
      });
      c.start();
   }

}
