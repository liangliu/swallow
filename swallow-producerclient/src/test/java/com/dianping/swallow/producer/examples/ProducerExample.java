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

/**
 * Producer示例
 * 
 * @author tong.song
 */
public class ProducerExample {
   public static void main(String[] args) {

      String message = "message";
      ProducerFactory producerFactory = null;

      //获取Producer工厂实例
      try {
         //OR: producerFactory = ProducerFactory.getInstance()//默认远程调用timeout为5000;
         producerFactory = ProducerFactoryImpl.getInstance(5000);
      } catch (RemoteServiceInitFailedException e) {
         //远程调用初始化失败抛出此异常
      }

      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);//如果不设置该项，默认为SYNC_MODE
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 5);//如果不设置该项，默认为5
      pOptions.put(ProducerOptionKey.IS_ZIP_MESSAGE, false);//如果希望存入DB前对消息进行压缩，此项设置为true，默认为false
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);//如果不设置该项，默认为5
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);//如果不设置该项，默认为false
      Producer producer = null;

      //获取Producer实例
      try {
         //OR: producer = producerFactory.getProducer(Destination.topic("example"));//使用默认设置获取Producer
         //默认Producer设置为：ProducerMode=SYNC_MODE，RetryTimes=5
         producer = producerFactory.getProducer(Destination.topic("example"), pOptions);
      } catch (TopicNameInvalidException e) {
         //Topic名称非法抛出此异常
      }

      //发送message
      try {
         producer.sendMessage(message);
      } catch (ServerDaoException e) {
         //消息保存到数据库出错
      } catch (FileQueueClosedException e) {
         //只存在于异步模式：消息保存到filequeue出错
      } catch (RemoteServiceDownException e) {
         //远程调用失败
      } catch (NullContentException e) {
         //待发送消息的内容为空
      }
   }
}
