package com.dianping.swallow.consumer.impl;

import org.junit.Assert;
import org.junit.Test;

import com.dianping.swallow.consumer.impl.ConsumerClientImpl;

public class ConsumerClientImplTest {

   @Test
   public void testGetAddressByParseLionValue(){
      ConsumerClientImpl consumerClient = new ConsumerClientImpl("zhangyu","xx");
      String address;
//      address = consumerClient.getAddressByParseLionValue("default=127.0.0.1:8081,127.0.0.1:8082;feed,topicForUnitTest=127.0.0.1:8083,127.0.0.1:8084", "xx");
//      Assert.assertTrue("127.0.0.1:8081,127.0.0.1:8082".equals(address));
//      address = consumerClient.getAddressByParseLionValue("default=127.0.0.1:8081,127.0.0.1:8082;", "xx");
//      Assert.assertTrue("127.0.0.1:8081,127.0.0.1:8082".equals(address));
//      address = consumerClient.getAddressByParseLionValue("default=127.0.0.1:8081,127.0.0.1:8082;xx,aa=127.0.0.1:8083,127.0.0.1:8084", "xx");
//      Assert.assertTrue("127.0.0.1:8083,127.0.0.1:8084".equals(address));
   }
  
}
