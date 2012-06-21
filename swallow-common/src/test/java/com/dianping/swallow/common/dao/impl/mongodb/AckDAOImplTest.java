package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class AckDAOImplTest extends AbstractDAOImplTest {

   @Autowired
   private AckDAOImpl ackDAOImpl;

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

   @Override
   protected String getDBName() {
      return this.getMongoConfig().getAckDBName();
   }

}
