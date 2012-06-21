package com.dianping.swallow.common.dao.impl.mongodb;

import junit.framework.Assert;

import org.bson.types.BSONTimestamp;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class AckDAOImplTest extends AbstractDAOImplTest {

   @Autowired
   private AckDAOImpl ackDAOImpl;

   private String     consumerId = "consumer1";

   @Test
   public void testAdd() {
      //添加一条记录
      int time = (int) (System.currentTimeMillis() / 1000);
      int inc = 1;
      BSONTimestamp timestamp = new BSONTimestamp(time, inc);
      Long expectedMessageId = BSONTimestampUtils.BSONTimestampToLong(timestamp);
      ackDAOImpl.add(TOPIC_NAME, consumerId, BSONTimestampUtils.BSONTimestampToLong(timestamp));
      //测试
      Long maxMessageId = ackDAOImpl.getMaxMessageId(TOPIC_NAME, consumerId);
      Assert.assertEquals(expectedMessageId, maxMessageId);
   }

   @Test
   public void testGetMaxMessageId() {
      //添加一条记录
      int time = (int) (System.currentTimeMillis() / 1000);
      int inc = 1;
      BSONTimestamp timestamp = new BSONTimestamp(time, inc);
      Long expectedMessageId = BSONTimestampUtils.BSONTimestampToLong(timestamp);
      ackDAOImpl.add(TOPIC_NAME, consumerId, BSONTimestampUtils.BSONTimestampToLong(timestamp));
      //测试
      Long maxMessageId = ackDAOImpl.getMaxMessageId(TOPIC_NAME, consumerId);
      Assert.assertEquals(expectedMessageId, maxMessageId);
   }

   @Override
   protected String getDBName() {
      return this.getMongoConfig().getAckDBName();
   }

}
