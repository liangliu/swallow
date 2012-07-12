package com.dianping.swallow.consumerserver.buffer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Map;

import junit.framework.Assert;

import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.internal.dao.AckDAO;
import com.dianping.swallow.common.internal.dao.impl.mongodb.AbstractDAOImplTest;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.consumerserver.worker.ConsumerId;
import com.dianping.swallow.consumerserver.worker.ConsumerInfo;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerImpl;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class ConsumerWorkerImplTest extends AbstractDAOImplTest {

   @Autowired
   private AckDAO                ackDAO;
   @Autowired
   private ConsumerWorkerManager consumerWorkerManager;
   private ConsumerWorkerImpl    consumerWorker;

   @Test
   public void testHandleGreet() {
      Channel channel = mock(Channel.class);
//      ChannelFuture channelFuture = mock(ChannelFuture.class);
      
      when(channel.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8081));
      when(channel.isConnected()).thenReturn(true);
      ConsumerId consumerId = new ConsumerId("zhangyu", Destination.topic("aaxx"));
      ConsumerInfo consumerInfo = new ConsumerInfo(consumerId, ConsumerType.AT_LEAST);
      consumerWorkerManager.handleGreet(channel, consumerInfo, 5, null);
      ConsumerWorkerImpl impl = (ConsumerWorkerImpl)consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId);
      Map<Channel, String> connectedChannel = impl.getConnectedChannels();
      Assert.assertTrue(connectedChannel.containsKey(channel));
   }
   //   private void getConsumerWorkerImpl(){

   //      consumerWorker = (ConsumerWorkerImpl)consumerWorkerManager.findOrCreateConsumerWorker(consumerInfo, null);
   //       
   //   }
   //   
   //   @Test
   //   public void testGetMessageIdOfTailMessage(){
   //      getConsumerWorkerImpl();
   //      Channel channel = mock(AbstractChannel.class);
   //      long maxMessageId;
   //      maxMessageId = consumerWorker.getMessageIdOfTailMessage("aaxx","zhangyu",channel);
   //      Assert.assertTrue(maxMessageId>0);
   //      maxMessageId = consumerWorker.getMessageIdOfTailMessage("xx","zhangyu",channel);
   //      Assert.assertTrue(maxMessageId>0);
   //      maxMessageId = consumerWorker.getMessageIdOfTailMessage("xx","meiyoude",channel);
   //      Assert.assertTrue(maxMessageId>0);
   //   }

}
