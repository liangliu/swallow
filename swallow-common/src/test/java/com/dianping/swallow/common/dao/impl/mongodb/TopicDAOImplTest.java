package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dianping.swallow.common.message.SwallowMessage;

@RunWith(SpringJUnit4ClassRunner.class)
public class TopicDAOImplTest {
   @Autowired
   private MessageDAOImpl topicDAOImpl;

   @Before
   public void init() {
      //      String uri = "mongodb://localhost:27017";
      //      MongoClient mongoClient = new MongoClient(uri, new MongoConfig());
      //      topicDAOImpl = new MessageDAOImpl(mongoClient.mongo.getDB("topic"));
   }

   //   @Test
   public void testSaveMessage() {
      //插入5条消息
      int i = 0;
      while (i++ < 5) {
         SwallowMessage message = createMessage();
         topicDAOImpl.saveMessage("topicB", message);
      }
   }

   @Test
   public void testGetMinMessages() {
      //查询messageId最小的消息
      List<SwallowMessage> minMessages = topicDAOImpl.getMinMessages("topicB", 2);
      System.out.println(minMessages);
   }

   //   @Test
   public void testGetMessagesGreaterThan() {
      //查询messageId比指定id大的按messageId升序排序的5条消息
      List<SwallowMessage> minMessages = topicDAOImpl.getMinMessages("topicB", 1);
      Long messageId = minMessages.get(0).getMessageId();
      List<SwallowMessage> messagesGreaterThan = topicDAOImpl.getMessagesGreaterThan("topicB", messageId, 5);
      System.out.println(messagesGreaterThan);
   }

   private static SwallowMessage createMessage() {
      SwallowMessage message = new SwallowMessage();
      message.setContent("this is a SwallowMessage");
      message.setGeneratedTime(new Date());
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      message.setProperties(map);
      message.setSha1("sha-1 string");
      message.setVersion("0.6.0");
      return message;

   }

}
