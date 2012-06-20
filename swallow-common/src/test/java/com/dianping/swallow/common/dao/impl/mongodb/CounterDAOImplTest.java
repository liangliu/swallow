package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public class CounterDAOImplTest {

   @Autowired
   private AckDAOImpl counterDAOImpl;

   @Before
   public void init() {
      //      String uri = "mongodb://localhost:27017";
      //      MongoClient mongoClient = new MongoClient(uri, new MongoConfig());
      //      counterDAOImpl = new AckDAOImpl(mongoClient.mongo.getDB("counter"));
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
