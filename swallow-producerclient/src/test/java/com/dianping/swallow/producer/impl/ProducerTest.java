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
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;

import sun.security.krb5.internal.crypto.Des;

import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.packet.PacketType;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;
import com.dianping.swallow.producer.impl.internal.SwallowPigeonConfiguration;

/**
 * Producer的单元测试，包含了对ProducerFactoryImpl和ProducerImpl类的测试
 * 
 * @author tong.song
 */
public class ProducerTest {
   private static ProducerSwallowService normalRemoteService    = mock(ProducerSwallowService.class);
   private static ProducerSwallowService exceptionRemoteService = mock(ProducerSwallowService.class);

   @BeforeClass
   public static void init() {
      PktSwallowPACK ack = new PktSwallowPACK("MockACK");
      when(normalRemoteService.sendMessage((Packet) anyObject())).thenReturn(ack);

      when(exceptionRemoteService.sendMessage((Packet) anyObject())).thenThrow(new ServerDaoException(null));

   }

   @Test
   public void testProducerConfig() {

      ProducerConfig producerConfig = new ProducerConfig();

      //测试默认值
      assertEquals(ProducerMode.SYNC_MODE, producerConfig.getMode());
      assertEquals(5, producerConfig.getRetryTimes());
      assertEquals(false, producerConfig.isZipped());
      assertEquals(5, producerConfig.getThreadPoolSize());
      assertEquals(false, producerConfig.isSendMsgLeftLastSession());

      //测试设置值
      producerConfig.setMode(ProducerMode.ASYNC_MODE);
      producerConfig.setRetryTimes(6);
      producerConfig.setZipped(true);
      producerConfig.setThreadPoolSize(6);
      producerConfig.setSendMsgLeftLastSession(true);

      assertEquals(ProducerMode.ASYNC_MODE, producerConfig.getMode());
      assertEquals(6, producerConfig.getRetryTimes());
      assertEquals(true, producerConfig.isZipped());
      assertEquals(6, producerConfig.getThreadPoolSize());
      assertEquals(true, producerConfig.isSendMsgLeftLastSession());

      //测试非法值
      producerConfig.setRetryTimes(-1);
      assertEquals(5, producerConfig.getRetryTimes());

      producerConfig.setThreadPoolSize(0);
      assertEquals(5, producerConfig.getThreadPoolSize());

      producerConfig.setThreadPoolSize(101);
      assertEquals(5, producerConfig.getThreadPoolSize());
   }

   @Test
   public void testSwallowPigeonConfiguration() {

      SwallowPigeonConfiguration defaultConfig = new SwallowPigeonConfiguration();

      //测试默认值
      assertEquals(SwallowPigeonConfiguration.DEFAULT_HOSTS, defaultConfig.getHosts());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_IS_USE_LION, defaultConfig.isUseLion());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERIALIZE, defaultConfig.getSerialize());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERVICE_NAME, defaultConfig.getServiceName());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_TIMEOUT, defaultConfig.getTimeout());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_WEIGHTS, defaultConfig.getWeights());

      //测试设置值
      defaultConfig.setHostsAndWeights("127.0.0.1:4999", "9");
      defaultConfig.setSerialize("what");
      defaultConfig.setServiceName("hello");
      defaultConfig.setTimeout(2222);
      defaultConfig.setUseLion(true);

      assertEquals("127.0.0.1:4999", defaultConfig.getHosts());
      assertEquals(true, defaultConfig.isUseLion());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERIALIZE, defaultConfig.getSerialize());
      assertEquals("hello", defaultConfig.getServiceName());
      assertEquals(2222, defaultConfig.getTimeout());
      assertEquals("9", defaultConfig.getWeights());

      //测试正常文件读取
      SwallowPigeonConfiguration normalConfig = new SwallowPigeonConfiguration("normalPigeon.properties");
      assertEquals("127.2.2.1:2000", normalConfig.getHosts());
      assertEquals(true, normalConfig.isUseLion());
      assertEquals("java", normalConfig.getSerialize());
      assertEquals("helloworld", normalConfig.getServiceName());
      assertEquals(200, normalConfig.getTimeout());
      assertEquals("2", normalConfig.getWeights());

      //测试格式错误文件读取
      SwallowPigeonConfiguration wrongConfig = new SwallowPigeonConfiguration("wrongPigeon.properties");
      assertEquals(SwallowPigeonConfiguration.DEFAULT_SERIALIZE, wrongConfig.getSerialize());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_TIMEOUT, wrongConfig.getTimeout());
      assertEquals(SwallowPigeonConfiguration.DEFAULT_IS_USE_LION, wrongConfig.isUseLion());
      assertEquals("127.2.2.1:2000,125.36.321.123:1325", wrongConfig.getHosts());
      assertEquals("2,1", wrongConfig.getWeights());
   }

   //测试ProducerFactory
   @Test
   public void testProducerFactoryCreateProducer() throws RemoteServiceInitFailedException {

      ProducerFactoryImpl producerFactory = null;
      //获取Producer工厂实例
      try {
         producerFactory = ProducerFactoryImpl.getInstance();
      } catch (RemoteServiceInitFailedException e) {
         throw e;
      }
      assertNotNull(producerFactory);

      //设置Producer选项
      ProducerImpl producer = null;
      ProducerConfig config = new ProducerConfig();

      //传入的config为null
      producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("UnitTest"), null);
      assertNotNull(producer);
      assertEquals(ProducerMode.SYNC_MODE, producer.getProducerConfig().getMode());
      assertEquals(5, producer.getProducerConfig().getRetryTimes());
      assertEquals(false, producer.getProducerConfig().isZipped());
      assertEquals(5, producer.getProducerConfig().getThreadPoolSize());
      assertEquals(false, producer.getProducerConfig().isSendMsgLeftLastSession());

      producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("UnitTest"), config);
      assertNotNull(producer);
      assertEquals(ProducerMode.SYNC_MODE, producer.getProducerConfig().getMode());
      assertEquals(5, producer.getProducerConfig().getRetryTimes());
      assertEquals(false, producer.getProducerConfig().isZipped());
      assertEquals(5, producer.getProducerConfig().getThreadPoolSize());
      assertEquals(false, producer.getProducerConfig().isSendMsgLeftLastSession());

      //测试创建异步模式的Producer
      producer = null;

      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(7);
      config.setZipped(true);
      config.setSendMsgLeftLastSession(true);
      config.setThreadPoolSize(100);

      producer = (ProducerImpl) producerFactory.createProducer(Destination.topic("UnitTest"), config);
      assertNotNull(producer);
      assertEquals(ProducerMode.ASYNC_MODE, producer.getProducerConfig().getMode());
      assertEquals(7, producer.getProducerConfig().getRetryTimes());
      assertEquals(true, producer.getProducerConfig().isZipped());
      assertEquals(100, producer.getProducerConfig().getThreadPoolSize());
      assertEquals(true, producer.getProducerConfig().isSendMsgLeftLastSession());
   }

   @Test
   public void testNomalAsyncProducerSendMessageWithoutTypeAndProperties() throws SendFailedException {

      ProducerConfig config = new ProducerConfig();

      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(2);
      config.setZipped(false);
      config.setSendMsgLeftLastSession(false);
      config.setThreadPoolSize(2);

      ProducerImpl producer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
            normalRemoteService, 5000);

      for (int i = 0; i < 10; i++) {
         String ret = producer.sendMessage("Hello UnitTest.");
         assertNull(ret);
      }
      try {
         Thread.sleep(2000);
      } catch (Exception e) {
      }
   }

   @Test
   public void testNormalAsyncProducerSendMessageWithTypeAndPropertiesWhileZippedIsOn() throws SendFailedException {

      ProducerConfig config = new ProducerConfig();

      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(2);
      config.setZipped(true);
      config.setSendMsgLeftLastSession(true);
      config.setThreadPoolSize(2);

      ProducerImpl producer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
            normalRemoteService, 5000);

      Map<String, String> properties = new HashMap<String, String>();
      properties.put("Hello", "World");
      properties.put("小猫", "你好");

      for (int i = 0; i < 10; i++) {
         String ret = producer.sendMessage("Hello UnitTest.", properties, "Test");
         assertNull(ret);
      }
      try {
         Thread.sleep(2000);
      } catch (Exception e) {
      }
   }

   @Test
   public void testExceptionAsyncProducerSendMessage() throws SendFailedException {

      ProducerConfig config = new ProducerConfig();

      config.setMode(ProducerMode.ASYNC_MODE);
      config.setRetryTimes(2);
      config.setZipped(false);
      config.setSendMsgLeftLastSession(false);
      config.setThreadPoolSize(2);

      ProducerImpl producer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
            exceptionRemoteService, 5000);

      for (int i = 0; i < 10; i++) {
         String ret = producer.sendMessage("Hello UnitTest.");
         assertNull(ret);
      }
   }

   //   @Test
   //   public void testAsyncProducerImpl() throws ServerDaoException {
   //      //正常的mock
   //      ProducerSwallowService normalRemoteServiceMock = mock(ProducerSwallowService.class);
   //      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
   //
   //      when(normalRemoteServiceMock.sendMessage(argThat(new Matcher<Packet>() {
   //         @Override
   //         public void describeTo(Description arg0) {
   //         }
   //
   //         @Override
   //         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
   //         }
   //
   //         @Override
   //         public boolean matches(Object arg0) {
   //            assertEquals(PacketType.OBJECT_MSG, ((Packet) arg0).getPacketType());
   //            System.out.println(((PktMessage) arg0).getContent().toString());
   //            return true;
   //         }
   //      }))).thenReturn(pktSwallowACK);
   //
   //      //抛异常的mock
   //      ProducerSwallowService exceptionRemoteServiceMock = mock(ProducerSwallowService.class);
   //      //设置异常remoteServiceMock的行为
   //      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(
   //            new SendFailedException(null));
   //
   //      //异步模式的pOptions
   //      ProducerConfig config = new ProducerConfig();
   //      config.setMode(ProducerMode.ASYNC_MODE);
   //      config.setRetryTimes(1);
   //      config.setZipped(false);
   //      config.setSendMsgLeftLastSession(false);
   //      config.setThreadPoolSize(2);
   //
   //      ProducerImpl normalProducer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
   //            normalRemoteServiceMock, 5000);
   //
   //      ProducerImpl exceptionProducer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
   //            exceptionRemoteServiceMock, 5000);
   //
   //      assertNotNull(normalProducer);
   //      assertNotNull(exceptionProducer);
   //      assertEquals(ProducerMode.ASYNC_MODE, normalProducer.getProducerConfig().getMode());
   //      assertEquals(ProducerMode.ASYNC_MODE, exceptionProducer.getProducerConfig().getMode());
   //      assertEquals(2, normalProducer.getProducerConfig().getRetryTimes());
   //      assertEquals(2, exceptionProducer.getProducerConfig().getRetryTimes());
   //      assertEquals(false, normalProducer.getProducerConfig().isZipped());
   //      assertEquals(false, exceptionProducer.getProducerConfig().isZipped());
   //      assertEquals(false, normalProducer.getProducerConfig().isSendMsgLeftLastSession());
   //      assertEquals(false, exceptionProducer.getProducerConfig().isSendMsgLeftLastSession());
   //      assertEquals(2, normalProducer.getProducerConfig().getThreadPoolSize());
   //      assertEquals(2, exceptionProducer.getProducerConfig().getThreadPoolSize());
   //
   //      //测试异步模式下抛出异常的Producer
   //      String strRet = "";
   //      try {
   //         for (int i = 0; i < 100; i++) {
   //            strRet = exceptionProducer.sendMessage("Hello World.");
   //            assertNull(strRet);
   //         }
   //      } catch (SendFailedException e) {
   //      }
   //
   //      //测试异步模式下情况正常的Producer
   //      strRet = "";
   //      try {
   //         for (int i = 0; i < 100; i++) {
   //            strRet = normalProducer.sendMessage("Hello World.");
   //            assertNull(strRet);
   //         }
   //      } catch (SendFailedException e) {
   //      }
   //   }
   //
   //   @Test
   //   public void testSyncProducerImpl() throws ServerDaoException {
   //
   //      //正常的mock
   //      ProducerSwallowService normalRemoteServiceMock = mock(ProducerSwallowService.class);
   //      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
   //      when(normalRemoteServiceMock.sendMessage(argThat(new Matcher<Packet>() {
   //         @Override
   //         public void describeTo(Description arg0) {
   //         }
   //
   //         @Override
   //         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
   //         }
   //
   //         @Override
   //         public boolean matches(Object arg0) {
   //            assertEquals(PacketType.OBJECT_MSG, ((Packet) arg0).getPacketType());
   //            System.out.println(((PktMessage) arg0).getContent().toString());
   //            return true;
   //         }
   //      }))).thenReturn(pktSwallowACK);
   //
   //      //抛异常的mock
   //      ProducerSwallowService exceptionRemoteServiceMock = mock(ProducerSwallowService.class);
   //      //设置异常remoteServiceMock的行为
   //      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(
   //            new ServerDaoException(null));
   //
   //      //同步模式的options
   //      ProducerConfig config = new ProducerConfig();
   //      config.setMode(ProducerMode.SYNC_MODE);
   //      config.setRetryTimes(1);
   //      config.setZipped(true);
   //
   //      ProducerImpl normalProducer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
   //            normalRemoteServiceMock, 5000);
   //
   //      ProducerImpl exceptionProducer = new ProducerImpl(Destination.topic("UnitTest"), config, "127.0.0.1", "0.6.0",
   //            exceptionRemoteServiceMock, 5000);
   //
   //      assertNotNull(normalProducer);
   //      assertEquals(true, normalProducer.getProducerConfig().isZipped());
   //      assertEquals(ProducerMode.SYNC_MODE, normalProducer.getProducerConfig().getMode());
   //      assertEquals(1, normalProducer.getProducerConfig().getRetryTimes());
   //
   //      assertNotNull(exceptionProducer);
   //      assertEquals(true, exceptionProducer.getProducerConfig().isZipped());
   //      assertEquals(ProducerMode.SYNC_MODE, exceptionProducer.getProducerConfig().getMode());
   //      assertEquals(1, exceptionProducer.getProducerConfig().getRetryTimes());
   //
   //      Map<String, String> properties = new HashMap<String, String>();
   //      properties.put("hello", "kitty");
   //      //测试同步模式正常情况下的Producer
   //      String strRet = null;
   //      try {
   //         strRet = normalProducer.sendMessage("Hello world.");
   //         assertEquals(pktSwallowACK.getShaInfo(), strRet);
   //         strRet = normalProducer.sendMessage("Hello world.", "Hello world.");
   //         assertEquals(pktSwallowACK.getShaInfo(), strRet);
   //         strRet = normalProducer.sendMessage("Hello world.", properties);
   //         assertEquals(pktSwallowACK.getShaInfo(), strRet);
   //      } catch (SendFailedException e) {
   //         System.out.println(e.getMessage());
   //      }
   //
   //      //测试同步模式下抛异常的Producer
   //      strRet = null;
   //      try {
   //         strRet = exceptionProducer.sendMessage("Hello world.");
   //      } catch (SendFailedException e) {
   //         assertNotNull(e);
   //      }
   //      assertNull(strRet);
   //   }
}
