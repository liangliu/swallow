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
 * Producer的单元测试，包含了对ProducerFactory和ProducerImpl类的测试
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

      //正常的mock
      MQService normalRemoteServiceMock = mock(MQService.class);
      PktSwallowPACK pktSwallowACK = new PktSwallowPACK("MockACK");
      when(normalRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenReturn(pktSwallowACK);

      //抛异常的mock
      MQService exceptionRemoteServiceMock = mock(MQService.class);

      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();

      //同步模式pOptions
      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
      pOptions.put(ProducerOptionKey.RETRY_TIMES, 5);

      ProducerImpl syncNormalProducer = null;
      ProducerImpl syncExceptionProducer = null;
      
      try {
         syncNormalProducer = new ProducerImpl(normalRemoteServiceMock, Destination.topic("UnitTest"), pOptions);
         syncExceptionProducer = new ProducerImpl(exceptionRemoteServiceMock, Destination.topic("UnitTest"), pOptions);
      } catch (Exception e) {
      }
      Assert.assertNotNull(syncNormalProducer);
      Assert.assertNotNull(syncExceptionProducer);

      String strRet = null;
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());
      try {
         strRet = syncNormalProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
      }
      Assert.assertEquals(pktSwallowACK.getShaInfo(), strRet);

      strRet = null;
      try {
         strRet = syncExceptionProducer.sendMessage("Hello world.");
      } catch (FileQueueClosedException e) {
      } catch (RemoteServiceDownException e) {
      } catch (NullContentException e) {
      } catch (ServerDaoException e) {
      }
      Assert.assertNull(strRet);
     
   }


   //   @Test
   //   public void testNormalProducer() {
   //      doTestProducer(normalRemoteService);
   //   }
   //
   //   @Test
   //   public void testExceptionProducer() {
   //      doTestProducer(exceptionRemoteService);
   //   }
   //
   //   public void doTestProducer(MQService remoteService) {
   //      Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
   //      Map<String, String> property = new HashMap<String, String>();
   //      property.put("test", "test");
   //      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
   //      ProducerImpl producer = null;
   //      try {
   //         producer = new ProducerImpl(remoteService, Destination.topic("testProducer"), pOptions);
   //      } catch (Exception e) {
   //      }
   //      Assert.assertNotNull(producer);
   //      String str = null;
   //      try {
   //         str = producer.sendMessage("testProducer");
   //      } catch (Exception e) {
   //      }
   //      Assert.assertNotNull(str);
   //
   //      Assert.assertEquals(ProducerMode.SYNC_MODE, producer.getProducerMode());
   //
   //      pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
   //      pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);
   //      pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);
   //      try {
   //         producer = new ProducerImpl(remoteService, Destination.topic("testNormalProducer"), pOptions);
   //      } catch (Exception e) {
   //      }
   //      Assert.assertNotNull(producer);
   //      str = null;
   //      try {
   //         str = producer.sendMessage("testNormalProducer", property, "type1");
   //         str = producer.sendMessage("testNormalProducer", property, "type2");
   //      } catch (Exception e) {
   //      }
   //      Assert.assertNull(str);
   //      Assert.assertEquals(ProducerMode.ASYNC_MODE, producer.getProducerMode());
   //      Assert.assertEquals(5, producer.getThreadPoolSize());
   //      Assert.assertEquals(false, producer.isContinueSend());
   //
   //      Assert.assertEquals("0.6.0", producer.getProducerVersion());
   //   }
   //   
   //   @Test
   //   public void testMockito(){
   //      MQService mockMQService = mock(MQService.class);
   //   }
}
