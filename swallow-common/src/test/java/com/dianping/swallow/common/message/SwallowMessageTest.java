package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

import com.dianping.swallow.common.internal.message.SwallowMessage;

public class SwallowMessageTest {

   @Test
   public void testTransferContentToBean() throws Exception {
      //自定义bean
      SwallowMessage msg = createMessage();
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");
      msg.setContent(demoBean);
      Assert.assertEquals("{\"a\":1,\"b\":\"b\"}", msg.getContent());
      Assert.assertEquals(demoBean, msg.transferContentToBean(DemoBean.class));
   }

   @Test
   public void testHashcode() throws Exception {
      //自定义bean
      SwallowMessage msg = createMessage();
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");
      msg.setGeneratedTime(null);
      msg.setContent(demoBean);
      Assert.assertEquals(32, msg.hashCode());
   }

   @Test
   public void testToString() throws Exception {
      //自定义bean
      SwallowMessage msg = createMessage();
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");
      msg.setGeneratedTime(null);
      msg.setContent(demoBean);
      Assert.assertEquals(
            "SwallowMessage [generatedTime=null, messageId=1, properties={property-key=property-value}, internalProperties={property-key=property-value}, version=0.6.0, content={\"a\":1,\"b\":\"b\"}, sha1=sha-1 string, type=feed, sourceIp=localhost]",
            msg.toString());
   }

   @Test
   public void testEquals() throws Exception {
      SwallowMessage msg = createMessage();
      msg.setGeneratedTime(null);
      SwallowMessage msg2 = createMessage();
      msg2.setGeneratedTime(null);
      Assert.assertTrue(msg.equals(msg2));
      msg2.setMessageId(2L);
      Assert.assertFalse(msg.equals(msg2));
   }

   @Test
   public void testEqualsWithoutMessageId() throws Exception {
      SwallowMessage msg = createMessage();
      msg.setGeneratedTime(null);
      SwallowMessage msg2 = createMessage();
      msg2.setGeneratedTime(null);
      Assert.assertTrue(msg.equals(msg2));
      msg2.setMessageId(2L);
      Assert.assertFalse(msg.equals(msg2));
      Assert.assertTrue(msg.equalsWithoutMessageId(msg2));
   }

   @Test
   public void testGetters() throws Exception {
      SwallowMessage msg = createMessage();
      Assert.assertEquals("this is a SwallowMessage", msg.getContent());
      Assert.assertEquals("sha-1 string", msg.getSha1());
      Assert.assertEquals("localhost", msg.getSourceIp());
      Assert.assertEquals("feed", msg.getType());
      Assert.assertEquals("0.6.0", msg.getVersion());
      Assert.assertNotNull(msg.getGeneratedTime());
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      Assert.assertEquals(map, msg.getInternalProperties());
      Assert.assertEquals(Long.valueOf(1L), msg.getMessageId());
      Assert.assertEquals(map, msg.getProperties());
   }

   private static SwallowMessage createMessage() {
      SwallowMessage message = new SwallowMessage();
      message.setMessageId(1L);
      message.setContent("this is a SwallowMessage");
      message.setGeneratedTime(new Date());
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      message.setProperties(map);
      message.setInternalProperties(map);
      message.setSha1("sha-1 string");
      message.setVersion("0.6.0");
      message.setType("feed");
      message.setSourceIp("localhost");
      return message;

   }

}

class DemoBean {

   private int    a;
   private String b;

   public DemoBean() {
      super();
   }

   public int getA() {
      return a;
   }

   public void setA(int a) {
      this.a = a;
   }

   public String getB() {
      return b;
   }

   public void setB(String b) {
      this.b = b;
   }

   @Override
   public String toString() {
      return String.format("DemoBean [a=%s, b=%s]", a, b);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + a;
      result = prime * result + ((b == null) ? 0 : b.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof DemoBean))
         return false;
      DemoBean other = (DemoBean) obj;
      if (a != other.a)
         return false;
      if (b == null) {
         if (other.b != null)
            return false;
      } else if (!b.equals(other.b))
         return false;
      return true;
   }

}
