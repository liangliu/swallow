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

import org.apache.zookeeper.proto.op_result_t;
import org.junit.Test;

import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;

/**
 * TODO Comment of ProducerTest
 * 
 * @author tong.song
 */
public class ProducerTest {
   @Test
   public void testNormalProducer() {
      MQServiceNormalMock normalRemoteService = new MQServiceNormalMock();
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      Producer producer = null;
      try {
         producer = new Producer(normalRemoteService, "testNormalProducer", pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(producer);
      String str = null;
      try {
         str = producer.sendMessage("testNormalProducer");
      } catch (Exception e) {
      }
      Assert.assertNotNull(str);
   }

   @Test
   public void testExceptionProducer() {
      MQServiceExceptionMock exceptionRemoteService = new MQServiceExceptionMock();
   }
}
