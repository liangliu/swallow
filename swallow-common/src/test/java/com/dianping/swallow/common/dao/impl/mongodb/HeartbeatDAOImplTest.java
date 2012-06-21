package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class HeartbeatDAOImplTest extends AbstractDAOImplTest {

   @Autowired
   private HeartbeatDAOImpl heartbeatDAOImpl;

   @Test
   public void testUpdateLastHeartbeat() {
      Date expectedDate = heartbeatDAOImpl.updateLastHeartbeat(IP);

      Date actualDate = heartbeatDAOImpl.findLastHeartbeat(IP);
      Assert.assertEquals(expectedDate, actualDate);

   }

   @Test
   public void testFindLastHeartbeat() {
      Date expectedDate = heartbeatDAOImpl.updateLastHeartbeat(IP);

      Date actualDate = heartbeatDAOImpl.findLastHeartbeat(IP);
      Assert.assertEquals(expectedDate, actualDate);

   }

}
