package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.dianping.swallow.common.message.SwallowMessage;

public class TopicDAOImplTest {

   private TopicDAOImpl topicDAOImpl;

   @Before
   public void init() {
      String uri = "mongodb://localhost:27017";
      MongoClient mongoClient = new MongoClient(uri, new ConfigManager());
      topicDAOImpl = new TopicDAOImpl(mongoClient.mongo.getDB("topic"));
   }

   @Test
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

   @Test
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
      Properties properties = new Properties();
      properties.setProperty("property-key", "property-value");
      message.getProperties().setProperty("property-key", "property-value");
      message.setRetryCount(1);
      message.setSha1("sha-1 string");
      message.setVersion("0.6.0");
      return message;

   }

}
