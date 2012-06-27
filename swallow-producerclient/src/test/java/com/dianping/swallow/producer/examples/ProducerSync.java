/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-26
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
package com.dianping.swallow.producer.examples;

import java.util.HashMap;
import java.util.Map;

import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;
import com.dianping.swallow.producer.impl.Producer;
import com.dianping.swallow.producer.impl.ProducerFactory;

/**
 * Producer同步模式示例
 * 
 * @author tong.song
 */
public class ProducerSync {
   public static void main(String[] args) {
      String message = "message";
      ProducerFactory producerFactory = null;
      //获取Producer工厂实例
      try {
         //Or: producerFactory = ProducerFactory.getInstance()//默认远程调用timeout为5000;
         producerFactory = ProducerFactory.getInstance(5000);
      } catch (Exception e) {
         System.out.println(e.toString());
      }
      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      Producer producer = null;
      //获取Producer实例
      try {
         producer = producerFactory.getProducer("example", pOptions);
      } catch (Exception e) {
         System.out.println(e.toString());
      }
      //发送message
      try {
         producer.sendMessage(message);
      } catch (Exception e) {
         System.out.println(e.toString());
      }
   }
}
