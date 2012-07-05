package com.dianping.swallow.producerserver.impl;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.SwallowMessage;

public class ProducerServerForTextTest {
   @Test
   public void testProducerServerForText() {
      //构造mock的文本对象
      TextObject textObj = new TextObject();
      textObj.setACK(true);
      textObj.setContent("This is a Mock Text content.");
      textObj.setTopic("UnitTest");

      //构造Channel
      Channel channel = mock(Channel.class);
      when(channel.write(Matchers.anyObject())).thenReturn(null);

      //构造MessageEvent对象，用以调用messageReceived方法
      SocketAddress socketAddress = new InetSocketAddress("127.0.0.1", 8000);
      MessageEvent messageEvent = mock(MessageEvent.class);
      when(messageEvent.getMessage()).thenReturn(textObj);
      when(messageEvent.getRemoteAddress()).thenReturn(socketAddress);
      when(messageEvent.getChannel()).thenReturn(channel);

      //构造MessageDAO的mock对象，用以初始化ProducerServerTextHandler
      MessageDAO messageDAO = mock(MessageDAO.class);

      //利用以上变量，构造ProducerServerTextHandler对象
      ProducerServerTextHandler producerServerTextHandler = new ProducerServerTextHandler(messageDAO);

      //测试发送消息
      producerServerTextHandler.messageReceived(null, messageEvent);

      textObj.setTopic("H:ello");
      producerServerTextHandler.messageReceived(null, messageEvent);
      
      doThrow(new RuntimeException()).when(messageDAO).saveMessage(Matchers.anyString(), (SwallowMessage)Matchers.anyObject());
      producerServerTextHandler.messageReceived(null, messageEvent);
   }
}
