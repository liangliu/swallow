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

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.producerserver.impl.ProducerServerForClient;
import com.dianping.swallow.producerserver.impl.ProducerServerForText;

/**
 * TODO Comment of ProducerServerBootstrap
 * @author tong.song
 *
 */
public class ProducerServerBootstrap {
   private ProducerServerForClient producerServerClient;
   private ProducerServerForText producerServerText;
   
   private ProducerServerBootstrap(){
      ApplicationContext ctx = new ClassPathXmlApplicationContext("producerServer.xml");
      producerServerClient = ctx.getBean("producerServerForClient", ProducerServerForClient.class);
      producerServerText = ctx.getBean("producerServerForText", ProducerServerForText.class);
   }
   
   public void start() throws Exception{
      producerServerClient.start();
      producerServerText.start();
   }
   
   public static void main(String[] args) {
      try {
         new ProducerServerBootstrap().start();
      } catch (Exception e) {
         System.out.println(e.toString());
      }
   }
}
