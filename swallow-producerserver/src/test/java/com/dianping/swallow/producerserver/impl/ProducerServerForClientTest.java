package com.dianping.swallow.producerserver.impl;

import static org.mockito.Mockito.*;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Date;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PacketType;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.util.SHAUtil;

public class ProducerServerForClientTest {
   @Test
   public void testProducerServerForClient(){
      //初始化ProducerServerForClient对象
      ProducerServerForClient producerServerForClient = new ProducerServerForClient();
      
      int port = 4000;
      producerServerForClient.setPort(port);

      Assert.assertEquals(port, producerServerForClient.getPort());

      //启动Service服务
      try {
         producerServerForClient.start();
      } catch (RemoteServiceInitFailedException e) {
      }
      
      //构造greet
      PktProducerGreet pktProducerGreet = new PktProducerGreet("0.6.0", "Unit Test");
      //构造message
      SwallowMessage swallowMessage = new SwallowMessage();
      swallowMessage.setContent("UnitTest");
      swallowMessage.setGeneratedTime(new Date());
      swallowMessage.setSourceIp("192.168.32.194");
      swallowMessage.setVersion("0.6.0");
      PktMessage pktMessage = new PktMessage(Destination.topic("UnitTest"), swallowMessage);
      
      //发送greet
      try {
         PktSwallowPACK greetACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktProducerGreet);
         Assert.assertEquals(PacketType.SWALLOW_P_ACK, greetACK.getPacketType());
         Assert.assertEquals(Inet4Address.getLocalHost().getHostAddress(), greetACK.getShaInfo());
      } catch (ServerDaoException e) {
         //一定不会抛出该异常
      } catch (UnknownHostException e) {
      }
      
      //不抛异常的DAO
      MessageDAO messageDAO = mock(MessageDAO.class);
      producerServerForClient.setMessageDAO(messageDAO);
      try {
         PktSwallowPACK messageACK = (PktSwallowPACK) producerServerForClient.sendMessage(pktMessage);
         Assert.assertEquals(PacketType.SWALLOW_P_ACK, messageACK.getPacketType());
         Assert.assertEquals(SHAUtil.generateSHA(swallowMessage.getContent()), messageACK.getShaInfo());
      } catch (ServerDaoException e) {
         //一定不会抛出该异常
      }
      
      //设置mock行为，抛出数据库异常
      when(messageDAO.saveMessage(Matchers.anyString(), (SwallowMessage)Matchers.anyObject())).thenThrow(new ServerDaoException());
      when(exceptionRemoteServiceMock.sendMessage((Packet) Matchers.anyObject())).thenThrow(new ServerDaoException());

   }

}
