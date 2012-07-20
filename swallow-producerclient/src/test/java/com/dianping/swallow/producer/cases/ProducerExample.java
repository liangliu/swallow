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
package com.dianping.swallow.producer.cases;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

/**
 * Producer示例
 * 
 * @author tong.song
 */
public class ProducerExample {
   public static void main(String[] args) throws Exception {

      //待发送消息
      String message = "Example";

      //获取Producer工厂实例
      ProducerFactory producerFactory = null;
      try {
         producerFactory = ProducerFactoryImpl.getInstance();
      } catch (RemoteServiceInitFailedException e) {
         //远程调用初始化失败抛出此异常
         throw e;
      }

      ProducerConfig config = new ProducerConfig();
      //配置Producer选项，如果配置项出错则使用默认配置
      //默认配置的Producer为同步模式（SYNC_MODE）、失败重试次数为5、压缩选项为假
      config.setMode(ProducerMode.SYNC_MODE);//Producer工作模式（同步/异步）
      config.setRetryTimes(3);//发送失败重试次数
      config.setZipped(false);//是否对待发送消息执行压缩
      //以下配置中，标*的选项只在异步模式生效
      config.setThreadPoolSize(3);//*线程池大小，默认为5
      config.setSendMsgLeftLastSession(false);//*是否续传，默认为否

      //获取Producer实例
      Producer producer = null;
      //OR: producer = producerFactory.getProducer(Destination.topic("example"));//使用默认设置获取Producer
      //默认Producer设置为：ProducerMode=SYNC_MODE，RetryTimes=5
      producer = producerFactory.createProducer(Destination.topic("Example"), config);

      //发送message
      try {
         producer.sendMessage(message);
      } catch (SendFailedException e) {
         //发送失败则抛出此异常
         e.printStackTrace();
      }
   }
}
