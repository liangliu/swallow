/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-25
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

import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dianping.dpsf.api.ProxyFactory;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.producer.ProducerFactoryIface;
import com.dianping.swallow.producer.ProducerOptionKey;

/**
 * TODO Comment of ProducerFactory
 * 
 * @author tong.song
 */
public class ProducerFactory implements ProducerFactoryIface {

   Logger                         logger          = Logger.getLogger(ProducerFactory.class);
   private static ProducerFactory instance;                                                 //Producer工厂类单例
   //远程调用相关设置
   private final int              remoteServiceTimeout;                                     //远程调用超时
   private static final int       DEFAULT_TIMEOUT = 5000;                                   //远程调用默认超时
   //远程调用相关变量
   @SuppressWarnings("rawtypes")
   private final ProxyFactory     pigeon          = new ProxyFactory();                     //pigeon代理对象
   private final MQService        remoteService;                                            //远程调用对象

   /**
    * Producer工厂类构造函数
    * 
    * @param timeout 远程调用超时
    * @throws Exception 远程调用初始化失败
    */
   private ProducerFactory(int timeout) throws Exception {
      this.remoteServiceTimeout = timeout;
      //初始化远程调用
      try {
         remoteService = initRemoteService(remoteServiceTimeout);
      } catch (Exception e) {
         logger.log(Level.ERROR, "[Producer]:[Init remote service failed.]", e.getCause());
         throw e;
      }
   }

   /**
    * 初始化远程调用服务，如果远程服务端连接失败，抛出异常
    * 
    * @return 远程调用服务的借口
    * @throws Exception 远程调用服务失败
    */
   private MQService initRemoteService(int remoteServiceTimeout) throws Exception {
      pigeon.setServiceName("remoteService");
      pigeon.setIface(MQService.class);
      pigeon.setSerialize("hessian");
      pigeon.setCallMethod("sync");
      pigeon.setTimeout(remoteServiceTimeout);

      //TODO 配置Lion支持
      pigeon.setUseLion(false);
      pigeon.setHosts("127.0.0.1:4000");
      pigeon.setWeight("1");

      pigeon.init();

      return (MQService) pigeon.getProxy();
   }

   /**
    * 获取Producer工厂类单例，默认超时
    * 
    * @return Producer工厂类单例
    * @throws Exception
    */
   public static ProducerFactory getInstance() throws Exception {
      return doGetInstance(-1);
   }

   /**
    * 获取Producer工厂类单例，指定超时，如果单例已存在，则指定的超时无效
    * 
    * @param remoteServiceTimeout 远程调用超时
    * @return Producer工厂类单例
    * @throws Exception
    */
   public static ProducerFactory getInstance(int remoteServiceTimeout) throws Exception {
      return doGetInstance(remoteServiceTimeout);
   }

   /**
    * 实际获取Producer工厂类单例的函数
    * 
    * @param timeout 远程调用超时，如果小于零，则使用默认超时
    * @return Producer工程类单例
    * @throws Exception
    */
   private static synchronized ProducerFactory doGetInstance(int timeout) throws Exception {
      if (instance == null) {
         if (timeout < 0)
            instance = new ProducerFactory(DEFAULT_TIMEOUT);
         else
            instance = new ProducerFactory(timeout);
      }
      return instance;
   }
   /**
    * 获取Producer实现类对象，通过Map指定Producer的选项，未指定的项使用Producer默认配置，
    * Producer默认配置如下：
    *    producerMode:ProducerMode.SYNC_MODE;
    *    threadPoolSize:10;
    *    continueSend:false;
    * @throws Exception Producer选项有误，不能生成Producer对象，则抛出异常
    */
   @Override
   public ProducerImpl getProducer(String topicName, Map<ProducerOptionKey, Object> pOptions) throws Exception {
      return new ProducerImpl(remoteService, topicName, pOptions);
   }
}
