/**
 * Project: swallow-producerserver
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
package com.dianping.swallow.producerserver.bootstrap;

import java.io.File;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.cat.Cat;

/**
 * ProducerServer的Bootstrap类，用以启动ProducerServer
 * 
 * @author tong.song
 */
public class ProducerServerBootstrap {
   private ProducerServerBootstrap() {
   }

   public static void main(String[] args) {
      //启动Cat
      Cat.initialize(new File("/data/appdatas/cat/client.xml"));

      new ClassPathXmlApplicationContext("applicationContext.xml");
   }
}
