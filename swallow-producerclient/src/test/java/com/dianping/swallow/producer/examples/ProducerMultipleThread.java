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
package com.dianping.swallow.producer.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

class ExampleTask implements Runnable {

   Producer producer;

   public ExampleTask(Producer producer) {
      this.producer = producer;
   }

   @Override
   public void run() {
      try {
         producer.sendMessage("Hello World.");
      } catch (ServerDaoException e) {
         //*只存在于同步模式，保存至数据库失败则抛出此异常
      } catch (FileQueueClosedException e) {
         //*只存在于异步模式，保存至filequeue失败则抛出此异常
      } catch (RemoteServiceDownException e) {
         //远程调用失败则抛出此异常
      } catch (NullContentException e) {
         //待发送消息体为空则抛出此异常
      }
   }
}

/**
 * 多线程调用不同类型的Producer示例
 * 
 * @author tong.song
 */
public class ProducerMultipleThread {
   public static void main(String[] args) {

      ExecutorService threadPool = Executors.newCachedThreadPool();

      //获取Producer工厂实例
      ProducerFactory producerFactory = null;
      try {
         producerFactory = ProducerFactoryImpl.getInstance();
      } catch (RemoteServiceInitFailedException e) {
         throw e;
         //远程调用初始化失败抛出此异常
      }

      ProducerConfig config = new ProducerConfig();
      //配置Producer选项，如果配置项出错则使用默认配置
      //默认配置的Producer为同步模式（SYNC_MODE）、失败重试次数为5、压缩选项为假
      config.setMode(ProducerMode.ASYNC_MODE);//Producer工作模式（同步/异步）
      config.setRetryTimes(3);//发送失败重试次数
      config.setZipped(false);//是否对待发送消息执行压缩
      //以下配置中，标*的选项只在异步模式生效
      config.setThreadPoolSize(3);//*线程池大小，默认为5
      config.setSendMsgLeftLastSession(false);//*是否续传，默认为否

      //获取Producer实例（异步模式）
      Producer producerAsync = null;
      try {
         producerAsync = producerFactory.getProducer(Destination.topic("Example"), config);
      } catch (TopicNameInvalidException e) {
         //TopicName非法则抛出此异常
      }

      //重新配置Producer选项
      config.setMode(ProducerMode.SYNC_MODE);
      config.setZipped(true);
      
      //获取Producer实例（同步模式）
      Producer producerSync = null;
      try {
         producerSync = producerFactory.getProducer(Destination.topic("Example"), config);
      } catch (TopicNameInvalidException e) {
         //TopicName非法则抛出此异常
      }
      if (producerAsync != null && producerSync != null) {
         threadPool.execute(new ExampleTask(producerAsync));
         threadPool.execute(new ExampleTask(producerSync));
      }
   }
}
