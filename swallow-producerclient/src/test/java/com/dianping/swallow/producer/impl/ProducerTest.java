/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-27
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.producer.impl;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;

/**
 * TODO Comment of ProducerTest
 * 
 * @author tong.song
 */
public class ProducerTest {
   MQServiceNormalMock    normalRemoteService    = new MQServiceNormalMock();
   MQServiceExceptionMock exceptionRemoteService = new MQServiceExceptionMock();

   @Test
   public void testProducerFactory() {
      ProducerFactoryImpl producerFactory = null;
      //获取Producer工厂实例
      try {
         producerFactory = ProducerFactoryImpl.getInstance(5000);
         producerFactory = ProducerFactoryImpl.getInstance();
      } catch (Exception e) {
         System.out.println(e.toString());
      }
      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);

      Assert.assertNotNull(producerFactory);
   }

   @Test
   public void testNormalProducer() {
      doTestProducer(normalRemoteService);
   }

   @Test
   public void testExceptionProducer() {
      doTestProducer(exceptionRemoteService);
   }

   public void doTestProducer(MQService remoteService) {
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      Map<String, String> property = new HashMap<String, String>();
      property.put("test", "test");
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      ProducerImpl producer = null;
      try {
         producer = new ProducerImpl(remoteService, "testProducer", pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(producer);
      String str = null;
      try {
         str = producer.sendMessage("testProducer");
      } catch (Exception e) {
      }
      Assert.assertNotNull(str);

      Assert.assertEquals(ProducerMode.SYNC_MODE, producer.getProducerMode());

      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);
      try {
         producer = new ProducerImpl(remoteService, "testNormalProducer", pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(producer);
      str = null;
      try {
         str = producer.sendMessage("testNormalProducer", property, "type1");
         str = producer.sendMessage("testNormalProducer", property, "type2");
      } catch (Exception e) {
      }
      Assert.assertNull(str);
      Assert.assertEquals(ProducerMode.ASYNC_MODE, producer.getProducerMode());
      Assert.assertEquals(5, producer.getThreadPoolSize());
      Assert.assertEquals(false, producer.isContinueSend());

      Assert.assertEquals("0.6.0", producer.getProducerVersion());
   }
}
