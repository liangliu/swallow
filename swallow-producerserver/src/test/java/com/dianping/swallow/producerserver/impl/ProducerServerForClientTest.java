package com.dianping.swallow.producerserver.impl;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Date;
import static org.mockito.Mockito.*;

import jmockmongo.MockMongo;
import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.swallow.common.internal.config.DynamicConfig;
import com.dianping.swallow.common.internal.config.impl.LionDynamicConfig;
import com.dianping.swallow.common.internal.dao.MessageDAO;
import com.dianping.swallow.common.internal.dao.impl.mongodb.MessageDAOImpl;
import com.dianping.swallow.common.internal.dao.impl.mongodb.MongoClient;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PacketType;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.packet.PktProducerGreet;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.util.SHAUtil;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

public class ProducerServerForClientTest {

   static MockMongo mockMongo;
   static MockMongo mockMongo2;

   @Test
   public void testProducerServerForClient() throws UnknownHostException {
      //初始化ProducerServerForClient对象
      ProducerServerForClient producerServerForClient = new ProducerServerForClient();

      int port = 4000;
      producerServerForClient.setPort(port);

      Assert.assertEquals(port, producerServerForClient.getPort());

      //启动Service服务
      producerServerForClient.start();

      //构造greet
      PktProducerGreet pktProducerGreet = new PktProducerGreet("0.6.0", "Unit Test");
      //构造message
      SwallowMessage swallowMessage = new SwallowMessage();
      swallowMessage.setContent("UnitTest");
      swallowMessage.setGeneratedTime(new Date());
      swallowMessage.setSourceIp("192.168.32.194");
      swallowMessage.setVersion("0.6.0");
      PktMessage pktMessage = new PktMessage(Destination.topic("UnitTest"), swallowMessage);

      PktSwallowPACK ACK = null;
      //发送greet
      ACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktProducerGreet);

      Assert.assertEquals(PacketType.SWALLOW_P_ACK, ACK.getPacketType());
      Assert.assertEquals(Inet4Address.getLocalHost().getHostAddress(), ACK.getShaInfo());

      //不抛异常的DAO
      MessageDAO messageDAO = mock(MessageDAO.class);
      producerServerForClient.setMessageDAO(messageDAO);
      ACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktMessage);
      Assert.assertEquals(PacketType.SWALLOW_P_ACK, ACK.getPacketType());
      Assert.assertEquals(SHAUtil.generateSHA(swallowMessage.getContent()), ACK.getShaInfo());

      //设置mock行为，抛出数据库异常
      doThrow(new RuntimeException()).when(messageDAO).saveMessage(Matchers.anyString(),
            (SwallowMessage) Matchers.anyObject());
      ACK = null;
      try {
         ACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktMessage);
      } catch (ServerDaoException e) {
      }
      Assert.assertNull(ACK);
   }

   @Test
   public void testProducerServerForClientWithRealDAO() throws UnknownHostException {
      //初始化ProducerServerForClient对象
      ProducerServerForClient producerServerForClient = new ProducerServerForClient();

      //构造message
      SwallowMessage swallowMessage = new SwallowMessage();
      swallowMessage.setContent("UnitTest");
      swallowMessage.setGeneratedTime(new Date());
      swallowMessage.setSourceIp("192.168.32.194");
      swallowMessage.setVersion("0.6.0");
      PktMessage pktMessage = new PktMessage(Destination.topic("UnitTest"), swallowMessage);

      PktSwallowPACK ACK = null;

      //不抛异常的DAO
      MessageDAOImpl messageDAOImpl = new MessageDAOImpl();

      //mock的lion配置
      DynamicConfig config = mock(LionDynamicConfig.class);
      when(config.get("swallow.mongo.producerServerURI")).thenReturn(
            "default=mongodb://127.0.0.1:27016;feed=mongodb://127.0.0.1:21017");
      when(config.get("swallow.mongo.msgCappedCollectionSize")).thenReturn("default=1024;feed,topicForUnitTest=1025");
      when(config.get("swallow.mongo.msgCappedCollectionMaxDocNum")).thenReturn(
            "default=1024;feed,topicForUnitTest=1025");
      when(config.get("swallow.mongo.ackCappedCollectionSize")).thenReturn("default=1024;feed,topicForUnitTest=1025");
      when(config.get("swallow.mongo.ackCappedCollectionMaxDocNum")).thenReturn(
            "default=1024;feed,topicForUnitTest=1025");
      when(config.get("swallow.mongo.heartbeatServerURI")).thenReturn("mongodb://localhost:24521");
      when(config.get("swallow.mongo.heartbeatCappedCollectionSize")).thenReturn("1025");
      when(config.get("swallow.mongo.heartbeatCappedCollectionMaxDocNum")).thenReturn("1025");

      //真实的mongoClient
      MongoClient mongoClient = new MongoClient("swallow.mongo.producerServerURI", config);
      messageDAOImpl.setMongoClient(mongoClient);
      producerServerForClient.setMessageDAO(messageDAOImpl);

      ACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktMessage);
      Assert.assertEquals(PacketType.SWALLOW_P_ACK, ACK.getPacketType());
      Assert.assertEquals(SHAUtil.generateSHA(swallowMessage.getContent()), ACK.getShaInfo());

      mongoClient.onConfigChange("swallow.mongo.producerServerURI",
            "default=mongodb://127.0.0.1:27017;feed=mongodb://127.0.0.1:21016");

      ACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktMessage);
      Assert.assertEquals(PacketType.SWALLOW_P_ACK, ACK.getPacketType());
      Assert.assertEquals(SHAUtil.generateSHA(swallowMessage.getContent()), ACK.getShaInfo());

   }

   @BeforeClass
   public static void beforeClass() {
      mockMongo = new MockMongo(27017);
      mockMongo.start();

      mockMongo2 = new MockMongo(27016);
      mockMongo2.start();
   }

   @AfterClass
   public static void afterClass() {
      mockMongo.stop();
      mockMongo2.stop();
   }

}
