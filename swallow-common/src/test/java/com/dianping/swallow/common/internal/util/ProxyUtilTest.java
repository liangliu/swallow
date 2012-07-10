package com.dianping.swallow.common.internal.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ProxyUtilTest {

   @Test
   public void testCreateProxyWithRetryMechanism() {
      TempClassForTest target = new TempClassForTest();
      target.c = "test";
      TempClassForTest proxyTarget = ProxyUtil.createProxyWithRetryMechanism(target, 1000);
      assertEquals(target.c, proxyTarget.getA());
   }

}

class TempClassForTest {
   int    i = 0;
   String c = null;

   public String getA() {
      if (i++ < 1) {
         throw new RuntimeException("this is a exception from UnitTest.");
      }
      return c;
   }
}
