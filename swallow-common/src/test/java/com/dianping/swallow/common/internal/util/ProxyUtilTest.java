package com.dianping.swallow.common.internal.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ProxyUtilTest {

   @Test
   public void testCreateProxyWithRetryMechanism() {
      TempClassForTest proxyTarget = ProxyUtil.createProxyWithRetryMechanism(TempClassForTest.class, 1000);
      assertEquals("A", proxyTarget.getA());
   }

}

class TempClassForTest {
   int i = 0;

   public String getA() {
      if (i++ < 1) {
         throw new RuntimeException("abc");
      }
      return "A";
   }
}
