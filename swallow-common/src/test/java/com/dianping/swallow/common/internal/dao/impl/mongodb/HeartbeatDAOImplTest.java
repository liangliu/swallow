package com.dianping.swallow.common.internal.dao.impl.mongodb;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.internal.dao.impl.mongodb.HeartbeatDAOImpl;

public class HeartbeatDAOImplTest extends AbstractDAOImplTest {

   @Autowired
   private HeartbeatDAOImpl heartbeatDAO;

   @Test
   public void testUpdateLastHeartbeat() {
      Date expectedDate = heartbeatDAO.updateLastHeartbeat(IP);

      Date actualDate = heartbeatDAO.findLastHeartbeat(IP);
      Assert.assertEquals(expectedDate, actualDate);

   }

   @Test
   public void testFindLastHeartbeat() {
      Date expectedDate = heartbeatDAO.updateLastHeartbeat(IP);

      Date actualDate = heartbeatDAO.findLastHeartbeat(IP);
      Assert.assertEquals(expectedDate, actualDate);

   }

}
