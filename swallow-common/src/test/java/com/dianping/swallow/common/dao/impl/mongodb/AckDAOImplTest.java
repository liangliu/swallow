package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext.xml")
public class AckDAOImplTest extends AbstractJUnit4SpringContextTests {

   @Autowired
   private AckDAOImpl          ackDAOImpl;

   @Autowired
   private MongoClient         mongoClient;

   private static final String DB_NAME    = "unittest";
   private static final String TOPIC_NAME = "topicForTest";

   private Mongo               mongo;

   @Before
   public void setUp() throws Exception {
      //创建db
      mongo = mongoClient.getMongo(TOPIC_NAME);
      DB db = mongo.getDB(DB_NAME);
      //在db下，创建Collection
      DBObject option = new BasicDBObject();
      option.put("capped", true);
      option.put("size",512);
      db.createCollection(TOPIC_NAME, option);
   }

   @After
   public void tearDown() throws Exception {
      //删除db
      mongo.getDB(DB_NAME).dropDatabase();
   }

   @Test
   public void testAdd() {
      //test add
      BSONTimestamp timestamp = new BSONTimestamp();
      System.out.println(timestamp);
      ackDAOImpl.add(TOPIC_NAME, "consumer1", BSONTimestampUtils.BSONTimestampToLong(timestamp));
   }

   @Test
   public void testGetMaxMessageId() {
      //test getMaxMessageId
      Long messageId = ackDAOImpl.getMaxMessageId(TOPIC_NAME, "consumer1");
      System.out.println(messageId);
      System.out.println(BSONTimestampUtils.longToBSONTimestamp(messageId).toString());
   }

}
