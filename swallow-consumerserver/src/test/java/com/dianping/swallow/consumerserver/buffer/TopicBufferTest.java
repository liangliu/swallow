package com.dianping.swallow.consumerserver.buffer;

import java.util.concurrent.BlockingQueue;

import org.junit.Test;

import com.dianping.swallow.common.message.Message;

public class TopicBufferTest {

   protected static final String TOPIC_NAME = "topicForUnitTest";

   @Test
   public void testDecode() throws Exception {
      //      String cid = "cid-1";
      //      TopicBuffer topicBuffer = TopicBuffer.getTopicBuffer(TOPIC_NAME);
      //      long messageIdOfTailMessage = 1L;
      //      BlockingQueue<Message> queue = topicBuffer.createMessageQueue(cid, messageIdOfTailMessage);
      //
      //      int i = 0;
      //      while (true) {
      //         Message m = queue.poll();
      //         if (m != null) {
      //            System.out.println("poll message " + (++i) + ":" + m);
      //            System.out.println("queue size:" + queue.size());
      //         }
      //
      //         Thread.sleep(700L);//睡眠
      //      }

   }

}
