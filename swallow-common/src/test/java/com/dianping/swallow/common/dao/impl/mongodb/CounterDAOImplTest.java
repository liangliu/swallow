package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;
import org.junit.Before;
import org.junit.Test;

public class CounterDAOImplTest {

   private CounterDAOImpl counterDAOImpl;

   @Before
   public void init() {
      String uri = "mongodb://localhost:27017";
      MongoClient mongoClient = new MongoClient(uri, new ConfigManager());
      counterDAOImpl = new CounterDAOImpl(mongoClient.mongo.getDB("counter"));
   }

   @Test
   public void testAdd() {
      //test add
      BSONTimestamp timestamp = new BSONTimestamp();
      System.out.println(timestamp);
      counterDAOImpl.add("topicB", "consumer3", BSONTimestampUtils.BSONTimestampToLong(timestamp));
   }

   @Test
   public void testGetMaxMessageId() {
      //test getMaxMessageId
      Long messageId = counterDAOImpl.getMaxMessageId("topicB", "consumer3");
      System.out.println(messageId);
      System.out.println(BSONTimestampUtils.longToBSONTimestamp(messageId).toString());
   }

}
