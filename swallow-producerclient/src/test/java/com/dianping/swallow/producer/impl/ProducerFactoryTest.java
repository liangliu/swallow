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

import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;

/**
 * TODO Comment of ProducerTest
 * @author tong.song
 *
 */
public class ProducerFactoryTest{
   @Test
   public void testProducerFactory(){
      ProducerFactory producerFactory = null;
      //获取Producer工厂实例
      try {
         producerFactory = ProducerFactory.getInstance(5000);
         producerFactory = ProducerFactory.getInstance();
      } catch (Exception e) {
         System.out.println(e.toString());
      }
      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      
      Assert.assertNotNull(producerFactory);
   }
}
