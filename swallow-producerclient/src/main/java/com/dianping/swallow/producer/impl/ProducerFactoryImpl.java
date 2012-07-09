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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.dpsf.api.ProxyFactory;
import com.dianping.dpsf.exception.NetException;
import com.dianping.swallow.common.internal.packet.PktProducerGreet;
import com.dianping.swallow.common.internal.producer.MQService;
import com.dianping.swallow.common.internal.util.IPUtil;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;

/**
 * Producer工厂类
 * 
 * @author tong.song
 */
public class ProducerFactoryImpl implements ProducerFactory {

   private static final Logger        logger          = LoggerFactory.getLogger(ProducerFactoryImpl.class);

   private static final String        producerIP      = IPUtil.getFirstNoLoopbackIP4Address();             //Producer IP地址
   private static final String        producerVersion = "0.6.0";                                           //Producer版本号

   private static ProducerFactoryImpl instance;                                                            //Producer工厂类单例

   //远程调用相关设置
   private static int                 remoteServiceTimeout;                                                //远程调用超时
   private static final int           DEFAULT_TIMEOUT = 5000;                                              //远程调用默认超时

   //远程调用相关变量
   @SuppressWarnings("rawtypes")
   private static final ProxyFactory  pigeon          = new ProxyFactory();                                //pigeon代理对象
   //TODO 为了单元测试，先将final设定去除掉了，后面有别的办法再补回来，或者就直接给Factory类提供一个setMQService方法？
   private static MQService           remoteService;                                                       //远程调用对象

   /**
    * Producer工厂类构造函数
    * 
    * @param timeout 远程调用超时
    * @throws RemoteServiceInitFailedException 远程调用初始化失败
    */
   private ProducerFactoryImpl(int timeout) throws RemoteServiceInitFailedException {
      remoteServiceTimeout = timeout;
      //初始化远程调用
      remoteService = initRemoteService(timeout);
   }

   /**
    * 初始化远程调用服务，如果远程服务端连接失败，抛出异常
    * 
    * @return 远程调用服务的接口
    * @throws RemoteServiceInitFailedException 远程调用初始化失败
    */
   private MQService initRemoteService(int remoteServiceTimeout) throws RemoteServiceInitFailedException {
      pigeon.setServiceName("remoteService");
      pigeon.setIface(MQService.class);
      pigeon.setSerialize("hessian");
      pigeon.setCallMethod("sync");
      pigeon.setTimeout(remoteServiceTimeout);

      //TODO 配置Lion支持
      pigeon.setUseLion(false);
      pigeon.setHosts("127.0.0.1:4000");
      pigeon.setWeight("1");

      MQService remoteService = null;
      try {
         pigeon.init();
         logger.info("[Initialize pigeon successfully.]");

         remoteService = (MQService) pigeon.getProxy();
         logger.info("[Get remoteService successfully.]:[" + "RemoteService's timeout is: " + remoteServiceTimeout
               + ".]");
      } catch (Exception e) {
         logger.error("[Initialize remote service failed.]", e);
         throw new RemoteServiceInitFailedException();
      }
      return remoteService;
   }

   /**
    * 获取Producer工厂类单例，默认超时
    * 
    * @return Producer工厂类单例
    * @throws RemoteServiceInitFailedException 远程调用初始化失败
    */
   public static ProducerFactoryImpl getInstance() throws RemoteServiceInitFailedException {
      return doGetInstance(-1);
   }

   /**
    * 获取Producer工厂类单例，指定超时，如果单例已存在，则指定的超时无效
    * 
    * @param remoteServiceTimeout 远程调用超时
    * @return Producer工厂类单例
    * @throws RemoteServiceInitFailedException 远程调用初始化失败
    */
   public static ProducerFactoryImpl getInstance(int remoteServiceTimeout) throws RemoteServiceInitFailedException {
      return doGetInstance(remoteServiceTimeout);
   }

   /**
    * 实际获取Producer工厂类单例的函数
    * 
    * @param timeout 远程调用超时，如果小于零，则使用默认超时
    * @return Producer工程类单例
    * @throws RemoteServiceInitFailedException 远程调用初始化失败
    */
   private static synchronized ProducerFactoryImpl doGetInstance(int timeout) throws RemoteServiceInitFailedException {
      if (instance == null) {
         if (timeout < 0)
            instance = new ProducerFactoryImpl(DEFAULT_TIMEOUT);
         else
            instance = new ProducerFactoryImpl(timeout);
      }
      return instance;
   }

   /**
    * @return 获取远程调用服务接口
    */
   @Override
   public MQService getRemoteService() {
      return remoteService;
   }

   /**
    * @return 获取Producer本机IP地址
    */
   @Override
   public String getProducerIP() {
      return producerIP;
   }

   /**
    * @return 获取Producer的版本号
    */
   @Override
   public String getProducerVersion() {
      return producerVersion;
   }

   /**
    * 获取默认配置的Producer，默认Producer工作模式为同步，重试次数为5
    * 
    * @throws TopicNameInvalidException topic名称非法//topic名称只能由字母、数字、下划线组成
    * @throws RemoteServiceInitFailedException Producer尝试连接远程服务失败
    */
   @Override
   public Producer getProducer(Destination dest) throws TopicNameInvalidException {
      return getProducer(dest, null);
   }

   /**
    * 获取Producer实现类对象，通过Map指定Producer的选项，未指定的项使用Producer默认配置， Producer默认配置如下：
    * producerMode:ProducerMode.SYNC_MODE; threadPoolSize:10;
    * continueSend:false; retryTimes:5
    * 
    * @throws TopicNameInvalidException topic名称非法//topic名称只能由字母、数字、下划线组成
    * @throws RemoteServiceInitFailedException Producer尝试连接远程服务失败
    */
   @Override
   public Producer getProducer(Destination dest, Map<ProducerOptionKey, Object> pOptions)
         throws TopicNameInvalidException {
      ProducerImpl producerImpl = null;
      try {
         producerImpl = new ProducerImpl(this, dest, pOptions);
         logger.info("[New producer instance was created.]:[topicName="
               + dest.getName()
               + "][ProducerMode="
               + producerImpl.getProducerMode()
               + (producerImpl.getProducerMode().equals(ProducerMode.ASYNC_MODE) ? "][ThreadPoolSize="
                     + producerImpl.getThreadPoolSize() + "][RetryTimes=" + producerImpl.getRetryTimes()
                     + "][IfContinueSend=" + producerImpl.isContinueSend() + "]" : "]"));
      } catch (TopicNameInvalidException e) {
         logger.error(
               "[Can not get producer instance.]:[topicName="
                     + dest.getName()
                     + "][ProducerMode="
                     + pOptions.get(ProducerOptionKey.PRODUCER_MODE)
                     + ((pOptions.get(ProducerOptionKey.PRODUCER_MODE) == ProducerMode.ASYNC_MODE) ? "][ThreadPoolSize="
                           + pOptions.get(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE)
                           + "][RetryTimes="
                           + pOptions.get(ProducerOptionKey.RETRY_TIMES)
                           + "][IfContinueSend="
                           + pOptions.get(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND) + "]"
                           : "]"), e);
         throw e;
      }

      //向swallow发送greet信息
      PktProducerGreet pktProducerGreet = new PktProducerGreet(producerVersion, producerIP);

      try {
         remoteService.sendMessage(pktProducerGreet);
      } catch (ServerDaoException e) {
         //一定不会捕获到该异常
      } catch (NetException e){
         //网络异常，不抛出，以保证用户可以拿到Producer
         logger.warn("[Network error, couldn't send greet now.]");
      }
      return producerImpl;
   }

   public void setRemoteService(MQService remoteService) {
      ProducerFactoryImpl.remoteService = remoteService;
   }

   public static int getRemoteServiceTimeout() {
      return remoteServiceTimeout;
   }

}
