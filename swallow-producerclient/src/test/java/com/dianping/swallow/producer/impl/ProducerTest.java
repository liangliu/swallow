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

import java.net.Inet4Address;
import java.net.UnknownHostException;
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

   //测试ProducerFactory
   @Test
   public void testProducerFactoryImpl() throws UnknownHostException {
      ProducerFactoryImpl producerFactory = null;

      SwallowService normalRemoteService = mock(SwallowService.class);

      //获取Producer工厂实例
      producerFactory = ProducerFactoryImpl.getInstance(100);
      producerFactory.setRemoteService(normalRemoteService);

      Assert.assertNotNull(producerFactory);
      Assert.assertEquals(100, ProducerFactoryImpl.getRemoteServiceTimeout());

      //设置Producer选项
      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 2);
      pOptions.put(ProducerOptionKey.IS_ZIP_MESSAGE, true);

      ProducerImpl producer = null;
      try {
         producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("Hello:Unit_Test"), pOptions);
      } catch (TopicNameInvalidException e) {
      }
      Assert.assertNull(producer);

      try {
         producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("Hello_Unit_Test"), pOptions);
      } catch (TopicNameInvalidException e) {
      }

      Assert.assertNotNull(producer);
      Assert.assertEquals(ProducerMode.SYNC_MODE, producer.getProducerMode());
      Assert.assertEquals(2, producer.getRetryTimes());
      Assert.assertEquals(true, producer.isZipMessage());

      producer = null;
      pOptions = new HashMap<ProducerOptionKey, Object>();

      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, true);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 100);

      try {
         producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("Hello"), pOptions);
      } catch (TopicNameInvalidException e) {
         //捕获到TopicNameInvalid异常
      }

      Assert.assertNotNull(producer);

      Assert.assertEquals("0.6.0", producerFactory.getProducerVersion());
      Assert.assertEquals(true, producer.isContinueSend());
      Assert.assertEquals(100, producer.getThreadPoolSize());
      Assert.assertEquals(Inet4Address.getLocalHost().getHostAddress(), producerFactory.getProducerIP());
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
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 2);
      pOptions.put(ProducerOptionKey.IS_ZIP_MESSAGE, false);
      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);
      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 2);

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
      Assert.assertEquals(ProducerMode.ASYNC_MODE, normalProducer.getProducerMode());
      Assert.assertEquals(ProducerMode.ASYNC_MODE, exceptionProducer.getProducerMode());
      Assert.assertEquals(2, normalProducer.getRetryTimes());
      Assert.assertEquals(2, exceptionProducer.getRetryTimes());
      Assert.assertEquals(false, normalProducer.isZipMessage());
      Assert.assertEquals(false, exceptionProducer.isZipMessage());
      Assert.assertEquals(false, normalProducer.isContinueSend());
      Assert.assertEquals(false, exceptionProducer.isContinueSend());
      Assert.assertEquals(2, normalProducer.getThreadPoolSize());
      Assert.assertEquals(2, exceptionProducer.getThreadPoolSize());

      //测试异步模式下抛出异常的Producer
      String strRet = "";
      //设置异常remoteServiceMock的行为
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());
      try {
         for (int i = 0; i < 100; i++) {
            strRet = exceptionProducer.sendMessage("Hello World.");
            Assert.assertNull(strRet);
         }
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      }

      //测试异步模式下情况正常的Producer
      strRet = "";
      try {
         for (int i = 0; i < 100; i++) {
            strRet = normalProducer.sendMessage("Hello World.");
            Assert.assertNull(strRet);
         }
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      }
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
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 2);

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
