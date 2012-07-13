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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.packet.PacketType;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerFactory;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;
import com.dianping.swallow.producer.impl.internal.SwallowPigeonConfiguration;

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

      ProducerSwallowService normalRemoteService = mock(ProducerSwallowService.class);

      //获取Producer工厂实例
      producerFactory = ProducerFactoryImpl.getInstance();
      producerFactory.setRemoteService(normalRemoteService);
      assertNotNull(producerFactory);

    //设置Producer选项
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.SYNC_MODE);
      config.setRetryTimes(2);
      config.setZipped(true);
      
      ProducerImpl producer = null;
      try {
         producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("Hello:Unit_Test"), config);
      } catch (TopicNameInvalidException e) {
      }
      assertNull(producer);

      try {
         producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("Hello_Unit_Test"), config);
      } catch (TopicNameInvalidException e) {
      }

      assertNotNull(producer);
      assertEquals(ProducerMode.SYNC_MODE, producer.getProducerConfig().getMode());
      assertEquals(2, producer.getProducerConfig().getRetryTimes());
      assertEquals(true, producer.getProducerConfig().isZipped());

      producer = null;

      config.setMode(ProducerMode.ASYNC_MODE);
      config.setSendMsgLeftLastSession(true);
      config.setThreadPoolSize(100);

      try {
         producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("Hello:Unit_Test"), config);
      } catch (TopicNameInvalidException e) {
      }
      assertNull(producer);

      try {
         producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("Hello"), config);
      } catch (TopicNameInvalidException e) {
         //捕获到TopicNameInvalid异常
      }

      assertNotNull(producer);

      assertEquals("0.6.0", producerFactory.getProducerVersion());
      assertEquals(Inet4Address.getLocalHost().getHostAddress(), producerFactory.getProducerIP());

      assertEquals(true, producer.getProducerConfig().isSendMsgLeftLastSession());
      assertEquals(100, producer.getProducerConfig().getThreadPoolSize());
   }

   @Test
   public void testSwallowPigeonConfiguration(){
      SwallowPigeonConfiguration defaultConfig = new SwallowPigeonConfiguration();
      assertEquals(SwallowPigeonConfiguration.DEFAULT_HOSTS, defaultConfig.getHosts());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_IS_USE_LION, defaultConfig.isUseLion());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERIALIZE, defaultConfig.getSerialize());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERVICE_NAME, defaultConfig.getServiceName());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_TIMEOUT, defaultConfig.getTimeout());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_WEIGHTS, defaultConfig.getWeights());
      
      defaultConfig.setHosts("127.0.0.1:4999");
      defaultConfig.setSerialize("what");
      defaultConfig.setServiceName("hello");
      defaultConfig.setTimeout(2222);
      defaultConfig.setUseLion(true);
      defaultConfig.setWeights("99");
      
      assertEquals("127.0.0.1:4999", defaultConfig.getHosts());
      assertEquals(true, defaultConfig.isUseLion());
      assertEquals("what", defaultConfig.getSerialize());
      assertEquals("hello", defaultConfig.getServiceName());
      assertEquals(2222, defaultConfig.getTimeout());
      assertEquals("99", defaultConfig.getWeights());
      
      SwallowPigeonConfiguration normalConfig = new SwallowPigeonConfiguration("normalPigeon.properties");
      assertEquals("127.2.2.1:2000", normalConfig.getHosts());
      assertEquals(true, normalConfig.isUseLion());
      assertEquals("java", normalConfig.getSerialize());
      assertEquals("helloworld", normalConfig.getServiceName());
      assertEquals(200, normalConfig.getTimeout());
      assertEquals("2", normalConfig.getWeights());
      
      SwallowPigeonConfiguration wrongConfig = new SwallowPigeonConfiguration("wrongPigeon.properties");
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERIALIZE, wrongConfig.getSerialize());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_TIMEOUT, wrongConfig.getTimeout());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_IS_USE_LION, wrongConfig.isUseLion());
      assertEquals("127.2.2.1:2000,125.36.321.123:1325", wrongConfig.getHosts());
      assertEquals("2,1", wrongConfig.getWeights());
   }
   
   @Test
   public void testAsyncProducerImpl() throws ServerDaoException {
      //正常的mock
      ProducerSwallowService normalRemoteServiceMock = mock(ProducerSwallowService.class);
      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
      
      when(normalRemoteServiceMock.sendMessage(argThat(new Matcher<Packet>() {
         @Override
         public void describeTo(Description arg0) {
         }
         @Override
         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
         }
         @Override
         public boolean matches(Object arg0) {
            assertEquals(PacketType.OBJECT_MSG, ((Packet)arg0).getPacketType());
            System.out.println(((PktMessage)arg0).getContent().toString());
            return true;
         }
      }))).thenReturn(pktSwallowACK);

      //抛异常的mock
      ProducerSwallowService exceptionRemoteServiceMock = mock(ProducerSwallowService.class);
      //设置异常remoteServiceMock的行为
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());

      //Normal ProducerFactory mock
      ProducerFactory normalProducerFactory = mock(ProducerFactory.class);
      when(normalProducerFactory.getRemoteService()).thenReturn(normalRemoteServiceMock);
      when(normalProducerFactory.getRemoteService()).thenReturn(normalRemoteServiceMock);
      when(normalProducerFactory.getProducerIP()).thenReturn("127.0.0.1");
      when(normalProducerFactory.getProducerVersion()).thenReturn("0.6.0");


      //Exception ProducerFactory mock
      ProducerFactory exceptionProducerFactory = mock(ProducerFactory.class);
      when(exceptionProducerFactory.getRemoteService()).thenReturn(exceptionRemoteServiceMock);
      when(exceptionProducerFactory.getProducerIP()).thenReturn("127.0.0.1");
      when(exceptionProducerFactory.getProducerVersion()).thenReturn("0.6.0");

      //异步模式的pOptions
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(2);
      config.setZipped(false);
      config.setSendMsgLeftLastSession(false);
      config.setThreadPoolSize(2);

      //异步模式的Producer
      ProducerImpl normalProducer = null;
      ProducerImpl exceptionProducer = null;

      //构造异步模式的Producer
      try {
         normalProducer = new ProducerImpl(normalProducerFactory, Destination.topic("UnitTest"), config);
         exceptionProducer = new ProducerImpl(exceptionProducerFactory, Destination.topic("UnitTest"), config);
      } catch (Exception e) {
      }
      
      assertNotNull(normalProducer);
      assertNotNull(exceptionProducer);
      assertEquals(ProducerMode.ASYNC_MODE, normalProducer.getProducerConfig().getMode());
      assertEquals(ProducerMode.ASYNC_MODE, exceptionProducer.getProducerConfig().getMode());
      assertEquals(2, normalProducer.getProducerConfig().getRetryTimes());
      assertEquals(2, exceptionProducer.getProducerConfig().getRetryTimes());
      assertEquals(false, normalProducer.getProducerConfig().isZipped());
      assertEquals(false, exceptionProducer.getProducerConfig().isZipped());
      assertEquals(false, normalProducer.getProducerConfig().isSendMsgLeftLastSession());
      assertEquals(false, exceptionProducer.getProducerConfig().isSendMsgLeftLastSession());
      assertEquals(2, normalProducer.getProducerConfig().getThreadPoolSize());
      assertEquals(2, exceptionProducer.getProducerConfig().getThreadPoolSize());
      
      //测试异步模式下抛出异常的Producer
      String strRet = "";
      try {
         for (int i = 0; i < 100; i++) {
            strRet = exceptionProducer.sendMessage("Hello World.");
            assertNull(strRet);
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
            assertNull(strRet);
         }
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      }
   }

   @Test
   public void testSyncProducerImpl() throws ServerDaoException {

      //正常的mock
      ProducerSwallowService normalRemoteServiceMock = mock(ProducerSwallowService.class);
      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
      when(normalRemoteServiceMock.sendMessage(argThat(new Matcher<Packet>() {
         @Override
         public void describeTo(Description arg0) {
         }
         @Override
         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
         }
         @Override
         public boolean matches(Object arg0) {
            assertEquals(PacketType.OBJECT_MSG, ((Packet)arg0).getPacketType());
            System.out.println(((PktMessage)arg0).getContent().toString());
            return true;
         }
      }))).thenReturn(pktSwallowACK);

      //抛异常的mock
      ProducerSwallowService exceptionRemoteServiceMock = mock(ProducerSwallowService.class);
      //设置异常remoteServiceMock的行为
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());

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
      ProducerConfig config = new ProducerConfig();
      config.setMode(ProducerMode.SYNC_MODE);
      config.setRetryTimes(1);
      config.setZipped(true);

      //构造Producer
      ProducerImpl normalProducer = null;
      ProducerImpl exceptionProducer = null;
      try {
         normalProducer = new ProducerImpl(normalProducerFactory, Destination.topic("UnitTest"), config);
         exceptionProducer = new ProducerImpl(exceptionProducerFactory, Destination.topic("UnitTest"), config);
      } catch (TopicNameInvalidException e) {
         System.out.println(e.getMessage());
      }

      assertNotNull(normalProducer);
      assertEquals(true, normalProducer.getProducerConfig().isZipped());
      assertEquals(ProducerMode.SYNC_MODE, normalProducer.getProducerConfig().getMode());
      assertEquals(1, normalProducer.getProducerConfig().getRetryTimes());

      assertNotNull(exceptionProducer);
      assertEquals(true, exceptionProducer.getProducerConfig().isZipped());
      assertEquals(ProducerMode.SYNC_MODE, exceptionProducer.getProducerConfig().getMode());
      assertEquals(1, exceptionProducer.getProducerConfig().getRetryTimes());

      Map<String, String> properties = new HashMap<String, String>();
      properties.put("hello", "kitty");
      //测试同步模式正常情况下的Producer
      String strRet = null;
      try {
         strRet = normalProducer.sendMessage("Hello world.");
         assertEquals(pktSwallowACK.getShaInfo(), strRet);
         strRet = normalProducer.sendMessage("Hello world.", "Hello world.");
         assertEquals(pktSwallowACK.getShaInfo(), strRet);
         strRet = normalProducer.sendMessage("Hello world.", properties);
         assertEquals(pktSwallowACK.getShaInfo(), strRet);
      } catch (FileQueueClosedException e) {
         System.out.println(e.getMessage());
      } catch (RemoteServiceDownException e) {
         System.out.println(e.getMessage());
      } catch (NullContentException e) {
         System.out.println(e.getMessage());
      } catch (ServerDaoException e) {
         System.out.println(e.getMessage());
      }

      //测试同步模式下抛异常的Producer
      strRet = null;
      try {
         strRet = exceptionProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
         assertNotNull(e);
      }
      assertNull(strRet);
   }
}
