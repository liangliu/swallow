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
         //Or: producerFactory = ProducerFactoryImpl.getInstance()//默认远程调用timeout为5000;
         producerFactory = ProducerFactoryImpl.getInstance(5000);
      } catch (RemoteServiceInitFailedException e) {
         //远程调用初始化失败抛出此异常
      }

      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 3);
      pOptions.put(ProducerOptionKey.IS_ZIP_MESSAGE, false);

      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 3);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);

      //获取Producer实例（异步模式）
      Producer producerAsync = null;
      try {
         producerAsync = producerFactory.getProducer(Destination.topic("Example"), pOptions);

         //重新配置Producer选项
         pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
         pOptions.put(ProducerOptionKey.RETRY_TIMES, 2);
         pOptions.put(ProducerOptionKey.IS_ZIP_MESSAGE, true);

         //获取Producer实例（同步模式）
         Producer producerSync = null;
         try {
            producerSync = producerFactory.getProducer(Destination.topic("Example"), pOptions);

            threadPool.execute(new ExampleTask(producerAsync));
            threadPool.execute(new ExampleTask(producerSync));
         } catch (TopicNameInvalidException e) {
            //TopicName非法则抛出此异常
         }

      } catch (TopicNameInvalidException e) {
         //TopicName非法则抛出此异常
      }
   }
}
