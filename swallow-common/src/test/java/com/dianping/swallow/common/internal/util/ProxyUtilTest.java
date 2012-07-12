package com.dianping.swallow.common.internal.util;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.MongoException;

public class ProxyUtilTest {

   @Test
   public void testCreateProxyWithRetryMechanism() {
      TempClassForTest target = new TempClassForTest();
      target.c = "test";
      TempClassForTest proxyTarget = ProxyUtil.createMongoDaoProxyWithRetryMechanism(target, 1000);
      Assert.assertEquals(target.c, proxyTarget.getA());
   }

   @Test
   public void testCreateProxyWithRetryMechanism2() {
      TempClassForTest target = new TempClassForTest();
      target.c = "test";
      TempClassForTest proxyTarget = ProxyUtil.createMongoDaoProxyWithRetryMechanism(target, 1000);
      try {
         proxyTarget.getB();
         Assert.fail();
      } catch (RuntimeException e) {
      }

   }

}

class TempClassForTest {
   int    i = 0;
   String c = null;

   public String getA() {
      if (i++ < 1) {
         throw new MongoException("this is a exception from UnitTest.");
      }
      return c;
   }

   public String getB() {
      if (i++ < 1) {
         throw new RuntimeException("this is a exception from UnitTest.");
      }
      return c;
   }
}
