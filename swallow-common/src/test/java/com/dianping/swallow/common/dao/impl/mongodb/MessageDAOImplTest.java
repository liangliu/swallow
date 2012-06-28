package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.message.SwallowMessage;

public class MessageDAOImplTest extends AbstractDAOImplTest {

   @Autowired
   private MessageDAOImpl messageDAO;

   @Test
   public void testSaveMessage() {
      //插入消息
      SwallowMessage expectedMessage = createMessage();
      expectedMessage.setContent("content in testSaveMessage");
      messageDAO.saveMessage(TOPIC_NAME, expectedMessage);
      //查询消息是否正确
      SwallowMessage actualMessage = messageDAO.getMaxMessage(TOPIC_NAME);
      Assert.assertTrue(expectedMessage.equalsWithoutMessageId(actualMessage));

   }

   @Test
   public void testGetMessage() {
      //插入消息
      SwallowMessage expectedMessage = createMessage();
      expectedMessage.setContent("content in testGetMessage");
      messageDAO.saveMessage(TOPIC_NAME, expectedMessage);
      //查询消息是否正确
      Long maxMessageId = messageDAO.getMaxMessageId(TOPIC_NAME);
      SwallowMessage actualMessage = messageDAO.getMessage(TOPIC_NAME, maxMessageId);
      Assert.assertTrue(expectedMessage.equalsWithoutMessageId(actualMessage));
   }

   @Test
   public void testGetMessagesGreaterThan() {
      //插入1条消息
      SwallowMessage message = createMessage();
      messageDAO.saveMessage(TOPIC_NAME, message);
      //获取消息id
      Long maxMessageId = messageDAO.getMaxMessageId(TOPIC_NAME);
      //再插入2条消息
      SwallowMessage expectedMessage1 = createMessage();
      messageDAO.saveMessage(TOPIC_NAME, expectedMessage1);
      SwallowMessage expectedMessage2 = createMessage();
      messageDAO.saveMessage(TOPIC_NAME, expectedMessage2);
      //查询messageId比指定id大的按messageId升序排序的2条消息
      List<SwallowMessage> messagesGreaterThan = messageDAO.getMessagesGreaterThan(TOPIC_NAME, maxMessageId, 5);
      Assert.assertNotNull(messagesGreaterThan);
      Assert.assertEquals(2, messagesGreaterThan.size());
      Assert.assertTrue(expectedMessage1.equalsWithoutMessageId(messagesGreaterThan.get(0)));
      Assert.assertTrue(expectedMessage2.equalsWithoutMessageId(messagesGreaterThan.get(1)));
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
      message.setType("feed");
      message.setSourceIp("localhost");
      return message;

   }

}
