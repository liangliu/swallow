package com.dianping.swallow.consumerserver.buffer;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.dianping.swallow.common.dao.impl.mongodb.MessageDAOImpl;
import com.dianping.swallow.common.dao.impl.mongodb.MongoClient;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext.xml")
public class SwallowBufferTest extends AbstractJUnit4SpringContextTests {
   private static final Logger   LOG        = LoggerFactory.getLogger(SwallowBufferTest.class);
   protected static final String TOPIC_NAME = "topicForUnitTest";

   @Autowired
   private SwallowBuffer         swallowBuffer;
   @Autowired
   private MessageDAOImpl        messageDAO;
   @Autowired
   private MongoClient           mongoClient;

   private String                cid        = "cid-1";

   private Long                  tailMessageId;

   @Before
   public void setUp() throws Exception {
      //插入1条消息
      SwallowMessage firstMsg = createMessage();
      firstMsg.setContent("content1");
      messageDAO.saveMessage(TOPIC_NAME, firstMsg);
      //初始化tailMessageId
      tailMessageId = messageDAO.getMaxMessageId(TOPIC_NAME);
      //添加20条Message
      int i = 2;
      while (i < 20) {
         //插入消息
         SwallowMessage msg = createMessage();
         msg.setContent("content" + i++);
         messageDAO.saveMessage(TOPIC_NAME, msg);
      }
   }

   @After
   public void tearDown() throws Exception {
      //删除测试过程创建的Collection
      mongoClient.getMessageCollection(TOPIC_NAME).drop();
   }

   @Test
   public void createMessageQueue() throws InterruptedException {
      BlockingQueue<Message> queue = swallowBuffer.createMessageQueue(TOPIC_NAME, cid, tailMessageId);

      int i = 0;
      while (true) {
         Message m = queue.poll(2, TimeUnit.SECONDS);
         if (m != null) {
            LOG.info("poll message " + (++i) + ":" + m);
            LOG.info("queue size:" + queue.size());
         }

      }
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
      return message;

   }

}
