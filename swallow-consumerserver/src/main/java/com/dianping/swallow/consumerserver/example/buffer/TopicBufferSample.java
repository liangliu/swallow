package com.dianping.swallow.consumerserver.example.buffer;

import java.util.concurrent.BlockingQueue;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumerserver.buffer.TopicBuffer;

public class TopicBufferSample {

   public static void main(String[] args) throws InterruptedException {

      String topicName = "topicA";
      Long cid = 1L;
      TopicBuffer topicBuffer = TopicBuffer.getTopicBuffer(topicName);
      long messageIdOfTailMessage = 1L;
      BlockingQueue<Message> queue = topicBuffer.createMessageQueue(cid, messageIdOfTailMessage);

      int i = 0;
      while (true) {
         Message m = queue.poll();
         if (m != null) {
            System.out.println("poll message " + (++i) + ":" + m);
            System.out.println("queue size:" + queue.size());
         }

         Thread.sleep(700L);//睡眠
      }

   }

}
