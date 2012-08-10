package com.dianping.swallow.producerserver.impl;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import junit.framework.Assert;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Test;
import org.mockito.Matchers;

import com.dianping.swallow.common.internal.dao.MessageDAO;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.util.SHAUtil;

public class ProducerServerForTextTest {
   @Test
   public void testProducerServerForText() {
      //构造mock的文本对象
      final TextObject textObj = new TextObject();
      textObj.setACK(true);
      textObj.setContent("This is a Mock Text content.");
      textObj.setTopic("UnitTest");

      SocketAddress socketAddress = new InetSocketAddress("127.0.0.1", 8000);
      
      //构造Channel
      Channel channel = mock(Channel.class);
      //Matchers.anyObject()
      when(channel.write(argThat(new Matcher<TextACK>() {
         @Override
         public void describeTo(Description arg0) {
         }
         @Override
         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
         }
         @Override
         public boolean matches(Object arg0) {
            TextACK textAck = (TextACK)arg0;
            System.out.println(textAck.toString());
            Assert.assertEquals(TextACK.class, arg0.getClass());
            switch(textAck.getStatus()){
               case ProducerServerTextHandler.OK:
                  Assert.assertEquals(SHAUtil.generateSHA(textObj.getContent()), textAck.getInfo());
                  break;
               case ProducerServerTextHandler.INVALID_TOPIC_NAME:
                  Assert.assertEquals("TopicName is invalid.", textAck.getInfo());
                  break;
               case ProducerServerTextHandler.SAVE_FAILED:
                  Assert.assertEquals("Can not save message.", textAck.getInfo());
                  break;
            }
            return true;
         }
      }))).thenReturn(null);
      when(channel.getRemoteAddress()).thenReturn(socketAddress);

      //构造MessageEvent对象，用以调用messageReceived方法
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
      
      ChannelEvent e = mock(ChannelStateEvent.class);
      ExceptionEvent e2 = mock (ExceptionEvent.class);
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      producerServerTextHandler.handleUpstream(ctx, e);
      producerServerTextHandler.exceptionCaught(ctx, e2);
      
      new ProducerServerForText().start();
   }
   
   @Test
   public void testProducerServerTextPipelineFactory(){
      MessageDAO messageDAO = mock(MessageDAO.class);
      ProducerServerTextPipelineFactory pstp = new ProducerServerTextPipelineFactory(messageDAO);
      System.out.println(pstp.getPipeline().toString());
   }
}
