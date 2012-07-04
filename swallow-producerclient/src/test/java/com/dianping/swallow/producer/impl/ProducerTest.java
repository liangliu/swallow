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
package com.dianping.swallow.producer.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;

/**
 * Producer的单元测试，包含了对ProducerFactoryImpl和ProducerImpl类的测试
 * 
 * @author tong.song
 */
public class ProducerTest {

   @Test
   public void testProducerFactoryImpl() {
      ProducerFactoryImpl producerFactory = null;

      MQService normalRemoteService = mock(MQService.class);

      //获取Producer工厂实例
      try {
         producerFactory = ProducerFactoryImpl.getInstance(5000);
      } catch (Exception e) {
      }

      producerFactory.setRemoteService(normalRemoteService);

      Assert.assertNotNull(producerFactory);

      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 5);

      ProducerImpl producer = null;

      try {
         producer = producerFactory.getProducer(Destination.topic("Hello:Unit_Test"), pOptions);
      } catch (TopicNameInvalidException e) {
         e.printStackTrace();
      } catch (RemoteServiceDownException e) {
         e.printStackTrace();
      }

      Assert.assertNull(producer);

      try {
         producer = producerFactory.getProducer(Destination.topic("Hello_Unit_Test"), pOptions);
      } catch (TopicNameInvalidException e) {
         e.printStackTrace();
      } catch (RemoteServiceDownException e) {
         e.printStackTrace();
      }

      Assert.assertNotNull(producer);

      producer = null;
      pOptions = null;
      pOptions = new HashMap<ProducerOptionKey, Object>();

      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 10);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);

      try {
         producer = producerFactory.getProducer(Destination.topic("H:ello"), pOptions);
      } catch (TopicNameInvalidException e) {
         //捕获到TopicNameInvalid异常
      } catch (RemoteServiceDownException e) {
         //一定不会捕获这个异常
      }

      Assert.assertNull(producer);

      try {
         producer = producerFactory.getProducer(Destination.topic("Hello"), pOptions);
      } catch (TopicNameInvalidException e) {
         //捕获到TopicNameInvalid异常
      } catch (RemoteServiceDownException e) {
         //一定不会捕获这个异常
      }

      Assert.assertNotNull(producer);
   }

   @Test
   public void testProducerImplNormal() throws ServerDaoException {
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();

      //正常的mock
      MQService normalRemoteServiceMock = mock(MQService.class);
      //设置正常mock的行为
      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
      when(normalRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenReturn(pktSwallowACK);

      //抛异常的mock（同步模式）
      MQService syncExceptionRemoteServiceMock = mock(MQService.class);
      //抛异常的mock（异步模式）
      MQService asyncExceptionRemoteServiceMock = mock(MQService.class);

      //同步模式pOptions
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 5);

      //同步模式的Producer
      ProducerImpl syncNormalProducer = null;
      ProducerImpl syncExceptionProducer = null;

      //构造同步模式的Producer
      try {
         syncNormalProducer = new ProducerImpl(normalRemoteServiceMock, Destination.topic("UnitTest"), pOptions);
         syncExceptionProducer = new ProducerImpl(syncExceptionRemoteServiceMock, Destination.topic("UnitTest"),
               pOptions);
      } catch (Exception e) {
      }
      Assert.assertEquals("0.6.0", syncNormalProducer.getProducerVersion());
      Assert.assertNotNull(syncNormalProducer);
      Assert.assertNotNull(syncExceptionProducer);

      //测试同步模式正常情况下的Producer
      String strRet = null;
      try {
         strRet = syncNormalProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
      }
      Assert.assertEquals(pktSwallowACK.getShaInfo(), strRet);

      //测试同步模式下抛异常的Producer
      strRet = null;
      //设置异常remoteServiceMock的行为
      when(syncExceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(
            new ServerDaoException());
      try {
         strRet = syncExceptionProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
      }
      Assert.assertNull(strRet);

      //异步模式的pOptions
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);

      //异步模式的Producer
      ProducerImpl asyncNormalProducer = null;
      ProducerImpl asyncExceptionProducer = null;

      //构造异步模式的Producer
      try {
         asyncNormalProducer = new ProducerImpl(normalRemoteServiceMock, Destination.topic("UnitTest"), pOptions);
         asyncExceptionProducer = new ProducerImpl(asyncExceptionRemoteServiceMock, Destination.topic("UnitTest"),
               pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(asyncNormalProducer);
      Assert.assertNotNull(asyncExceptionProducer);

      //测试异步模式下抛出异常的Producer
      strRet = "";
      //设置异常remoteServiceMock的行为
      when(asyncExceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(
            new ServerDaoException());
      try {
         for (int i = 0; i < 100; i++) {
            strRet = asyncExceptionProducer.sendMessage("Hello World.");
         }
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      }
      Assert.assertNull(strRet);

      //测试异步模式下情况正常的Producer//PS:启用消息压缩
      strRet = "";
      Map<String, String> properties = new HashMap<String, String>();
      properties.put("zip", "true");
      try {
         for (int i = 0; i < 100; i++) {
            strRet = asyncNormalProducer.sendMessage("Hello World.", properties);
         }
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      }
      Assert.assertNull(strRet);
   }
}
