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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.dpsf.api.ProxyFactory;
import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.LionException;
import com.dianping.swallow.common.internal.packet.PktProducerGreet;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.internal.util.IPUtil;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;
import com.dianping.swallow.producer.impl.internal.SwallowPigeonConfiguration;

/**
 * 实现ProducerFactory接口的工厂类
 * 
 * @author tong.song
 */
public class ProducerFactoryImpl implements ProducerFactory {

   private static ProducerFactoryImpl       instance;                                                             //Producer工厂类单例
   private static final Logger              logger           = LoggerFactory.getLogger(ProducerFactoryImpl.class);

   private static final String              CONFIG_FILE_NAME = "swallow-producerclient-pigeon.properties";        //配置文件名称

   private final String                     producerIP       = IPUtil.getFirstNoLoopbackIP4Address();             //Producer IP地址
   private final String                     producerVersion  = "0.6.0";                                           //Producer版本号
   private final SwallowPigeonConfiguration pigeonConfigure;                                                      //ProducerFactory配置类
   private final ProducerSwallowService     remoteService;                                                        //远程调用对象

   /**
    * Producer工厂类构造函数
    * 
    * @throws RemoteServiceInitFailedException 远程调用服务（pigeon）初始化失败
    */
   protected ProducerFactoryImpl() throws RemoteServiceInitFailedException {
      //初始化远程调用
      pigeonConfigure = new SwallowPigeonConfiguration(CONFIG_FILE_NAME);
      remoteService = initPigeon(pigeonConfigure);
   }

   /**
    * 初始化远程调用服务（pigeon）
    * 
    * @param pigeonConfigure ProducerFactory配置对象
    * @return 实现MQService接口的类，此版本中为pigeon返回的一个远程调用服务代理
    * @throws RemoteServiceInitFailedException 远程调用服务（pigeon）初始化失败
    */
   private ProducerSwallowService initPigeon(SwallowPigeonConfiguration pigeonConfigure)
         throws RemoteServiceInitFailedException {

      ProxyFactory<ProducerSwallowService> pigeon = new ProxyFactory<ProducerSwallowService>(); //pigeon代理对象
      pigeon.setIface(ProducerSwallowService.class);
      pigeon.setCallMethod("sync");

      pigeon.setServiceName(pigeonConfigure.getServiceName());
      pigeon.setSerialize(pigeonConfigure.getSerialize());
      pigeon.setTimeout(pigeonConfigure.getTimeout());
      pigeon.setUseLion(pigeonConfigure.isUseLion());
      pigeon.setHosts(pigeonConfigure.getHosts());
      pigeon.setWeight(pigeonConfigure.getWeights());

      ProducerSwallowService remoteService = null;
      try {
         ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());

         pigeon.init();
         logger.info("[Initialize pigeon successfully.]");

         remoteService = (ProducerSwallowService) pigeon.getProxy();
         logger.info("[Get remoteService successfully.]:[" + "RemoteService's timeout is: "
               + pigeonConfigure.getTimeout() + ".]");
      } catch (Exception e) {
         logger.error("[Initialize remote service failed.]", e);
         throw new RemoteServiceInitFailedException(e);
      }
      return remoteService;
   }

   /**
    * 获取Producer工厂类单例的函数
    * 
    * @return Producer工程类单例
    * @throws RemoteServiceInitFailedException 远程调用服务（pigeon）初始化失败
    */
   public static synchronized ProducerFactoryImpl getInstance() throws RemoteServiceInitFailedException {
      if (instance == null)
         instance = new ProducerFactoryImpl();
      return instance;
   }

   /**
    * 获取指定{@link Destination}，默认{@link ProducerConfig}的Producer
    */
   @Override
   public Producer createProducer(Destination dest) {
      return createProducer(dest, new ProducerConfig());
   }

   /**
    * 获取Producer实现类对象，未指定config时使用Producer默认配置
    */
   @Override
   public Producer createProducer(Destination dest, ProducerConfig config) {
      if (dest == null)
         throw new IllegalArgumentException("Destination can not be null");

      ProducerImpl producerImpl = null;
      producerImpl = new ProducerImpl(dest, config, producerIP, producerVersion, remoteService,
            pigeonConfigure.getTimeout());
      logger.info("New producer:[TopicName=" + dest.getName() + "; " + producerImpl.getProducerConfig().toString()
            + "]");
      //向swallow发送greet信息
      PktProducerGreet pktProducerGreet = new PktProducerGreet(producerVersion, producerIP);

      try {
         remoteService.sendMessage(pktProducerGreet);
      } catch (Exception e) {
         //吃掉所有异常，以保证用户可以拿到Producer
         logger.warn("Couldn't send greet now.", e);
      }

      return producerImpl;
   }
}
