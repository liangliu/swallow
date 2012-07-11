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

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.producer.SwallowService;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;

/**
 * Producer的单元测试，包含了对ProducerFactoryImpl和ProducerImpl类的测试
 * 
 * @author tong.song
 */
public class ProducerTest {

   @Test
   public void testProducerFactoryImpl() {
      ProducerFactoryImpl producerFactory = null;

      SwallowService normalRemoteService = mock(SwallowService.class);

      //获取Producer工厂实例
      producerFactory = ProducerFactoryImpl.getInstance(5000);
      producerFactory.setRemoteService(normalRemoteService);
      Assert.assertNotNull(producerFactory);

      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 7);

      ProducerImpl producer = null;

      producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("Hello:Unit_Test"), pOptions);

      Assert.assertNull(producer);

      producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("Hello_Unit_Test"), pOptions);

      Assert.assertNotNull(producer);

      Assert.assertEquals(ProducerMode.SYNC_MODE, producer.getProducerMode());
      Assert.assertEquals(7, producer.getRetryTimes());

      producer = null;
      pOptions = null;
      pOptions = new HashMap<ProducerOptionKey, Object>();

      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 8);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);

      try {
         producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("H:ello"), pOptions);
      } catch (TopicNameInvalidException e) {
         //捕获到TopicNameInvalid异常
      }

      Assert.assertNull(producer);

      try {
         producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("Hello"), pOptions);
      } catch (TopicNameInvalidException e) {
         //捕获到TopicNameInvalid异常
      }

      Assert.assertNotNull(producer);

      Assert.assertEquals(8, producer.getThreadPoolSize());
      Assert.assertEquals(false, producer.isContinueSend());
      Assert.assertEquals("0.6.0", producerFactory.getProducerVersion());

   }

   @Test
   public void testAsyncProducerImpl() throws ServerDaoException {
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();

      //正常的mock
      SwallowService normalRemoteServiceMock = mock(SwallowService.class);
      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
      when(normalRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenReturn(pktSwallowACK);

      //抛异常的mock
      SwallowService exceptionRemoteServiceMock = mock(SwallowService.class);

      //Normal ProducerFactory mock
      ProducerFactory normalProducerFactory = mock(ProducerFactory.class);
      when(normalProducerFactory.getRemoteService()).thenReturn(normalRemoteServiceMock);
      when(normalProducerFactory.getProducerIP()).thenReturn("127.0.0.1");
      when(normalProducerFactory.getProducerVersion()).thenReturn("0.6.0");

      //Exception ProducerFactory mock
      ProducerFactory exceptionProducerFactory = mock(ProducerFactory.class);
      when(exceptionProducerFactory.getRemoteService()).thenReturn(exceptionRemoteServiceMock);
      when(exceptionProducerFactory.getProducerIP()).thenReturn("127.0.0.1");
      when(exceptionProducerFactory.getProducerVersion()).thenReturn("0.6.0");

      //异步模式的pOptions
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);

      //异步模式的Producer
      ProducerImpl normalProducer = null;
      ProducerImpl exceptionProducer = null;

      //构造异步模式的Producer
      try {
         normalProducer = new ProducerImpl(normalProducerFactory, Destination.topic("UnitTest"), pOptions);
         exceptionProducer = new ProducerImpl(exceptionProducerFactory, Destination.topic("UnitTest"), pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(normalProducer);
      Assert.assertNotNull(exceptionProducer);

      //测试异步模式下抛出异常的Producer
      String strRet = "";
      //设置异常remoteServiceMock的行为
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());
      try {
         for (int i = 0; i < 100; i++) {
            strRet = exceptionProducer.sendMessage("Hello World.");
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
            strRet = normalProducer.sendMessage("Hello World.", properties);
         }
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      }
      Assert.assertNull(strRet);
   }

   @Test
   public void testSyncProducerImpl() throws ServerDaoException {

      //正常的mock
      SwallowService normalRemoteServiceMock = mock(SwallowService.class);
      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
      when(normalRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenReturn(pktSwallowACK);

      //抛异常的mock
      SwallowService exceptionRemoteServiceMock = mock(SwallowService.class);

      //Normal ProducerFactory mock
      ProducerFactory normalProducerFactory = mock(ProducerFactory.class);
      when(normalProducerFactory.getRemoteService()).thenReturn(normalRemoteServiceMock);
      when(normalProducerFactory.getProducerIP()).thenReturn("127.0.0.1");
      when(normalProducerFactory.getProducerVersion()).thenReturn("0.6.0");

      //Exception ProducerFactory mock
      ProducerFactory exceptionProducerFactory = mock(ProducerFactory.class);
      when(exceptionProducerFactory.getRemoteService()).thenReturn(exceptionRemoteServiceMock);
      when(exceptionProducerFactory.getProducerIP()).thenReturn("127.0.0.1");
      when(exceptionProducerFactory.getProducerVersion()).thenReturn("0.6.0");

      //同步模式的options
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 5);

      //构造Producer
      ProducerImpl normalProducer = null;
      ProducerImpl exceptionProducer = null;
      try {
         normalProducer = new ProducerImpl(normalProducerFactory, Destination.topic("UnitTest"), pOptions);
         exceptionProducer = new ProducerImpl(exceptionProducerFactory, Destination.topic("UnitTest"), pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(normalProducer);
      Assert.assertNotNull(exceptionProducer);

      //测试同步模式正常情况下的Producer
      String strRet = null;
      try {
         strRet = normalProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
      }
      Assert.assertEquals(pktSwallowACK.getShaInfo(), strRet);

      //测试同步模式下抛异常的Producer
      strRet = null;
      //设置异常remoteServiceMock的行为
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());
      try {
         strRet = exceptionProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
      }
      Assert.assertNull(strRet);
   }

}
