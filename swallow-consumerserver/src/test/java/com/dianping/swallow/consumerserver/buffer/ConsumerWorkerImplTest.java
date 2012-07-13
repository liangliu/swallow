package com.dianping.swallow.consumerserver.buffer;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.Assert;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.internal.consumer.ACKHandlerType;
import com.dianping.swallow.common.internal.dao.impl.mongodb.AbstractDAOImplTest;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumerserver.worker.ConsumerId;
import com.dianping.swallow.consumerserver.worker.ConsumerInfo;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerImpl;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class ConsumerWorkerImplTest extends AbstractDAOImplTest {

   @Autowired
   private ConsumerWorkerManager consumerWorkerManager;
   private ConsumerWorkerImpl    consumerWorker1;
   private ConsumerWorkerImpl    consumerWorker2;
   private BlockingQueue<Message>                 messageQueue      = new LinkedBlockingQueue<Message>();
   private Set<SwallowMessage>      messageSet = new HashSet<SwallowMessage>();
   private Set<SwallowMessage>      messageSet2 = new HashSet<SwallowMessage>();
   
   private void makeMessages(){
      for(long i=0;i<50;i++ ){
         SwallowMessage message = new SwallowMessage();
         message.setMessageId(i);
         messageSet.add(message);
         messageQueue.add(message);
      }
      
   }
   private void makeMessage(){
      
         SwallowMessage message = new SwallowMessage();
         message.setMessageId(123456L);
         messageSet2= new HashSet<SwallowMessage>();
         messageSet.add(message);
         messageQueue.add(message);
      
      
   }
   @Test
   public void testHandleGreet() throws InterruptedException {
      
      makeMessages();
      Channel channel = mock(Channel.class);
      when(channel.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8081));
      when(channel.isConnected()).thenReturn(true);
      when(channel.write(argThat(new Matcher<Object>() {
         @Override
         public void describeTo(Description arg0) {
            // TODO Auto-generated method stub            
         }
         @Override
         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
            // TODO Auto-generated method stub           
         }
         @Override
         public boolean matches(Object arg0) {
            messageSet2.add(((PktMessage)arg0).getContent());
            return true;
         }
      }))).thenReturn(null);
      
      ConsumerId consumerId2 = new ConsumerId("zhangyu2", Destination.topic("zhangyuxx2"));
      ConsumerInfo consumerInfo2 = new ConsumerInfo(consumerId2, ConsumerType.AT_MOST_ONCE);      
      consumerWorker2 = (ConsumerWorkerImpl)consumerWorkerManager.findOrCreateConsumerWorker(consumerInfo2, null);
      consumerWorker2.setMessageQueue(messageQueue);
      consumerWorkerManager.handleGreet(channel, consumerInfo2, 50, null);
      Thread.sleep(3000);
      Assert.assertTrue(messageSet.equals(messageSet2));
      Assert.assertTrue(consumerWorker2.getConnectedChannels().containsKey(channel));
      
      
      makeMessage();
      consumerWorkerManager.handleAck(channel, consumerInfo2, 50L, ACKHandlerType.SEND_MESSAGE);
      Thread.sleep(3000);
      Assert.assertTrue(messageSet2.size() ==1);
      for(SwallowMessage message : messageSet2){
         Assert.assertTrue(message.getMessageId().equals(123456L));
      }
      
      makeMessages();
      ConsumerId consumerId1 = new ConsumerId("zhangyu1", Destination.topic("zhangyuxx1"));
      ConsumerInfo consumerInfo1 = new ConsumerInfo(consumerId1, ConsumerType.AT_LEAST_ONCE);      
      consumerWorker1 = (ConsumerWorkerImpl)consumerWorkerManager.findOrCreateConsumerWorker(consumerInfo1, null);
      consumerWorker1.setMessageQueue(messageQueue);
      consumerWorkerManager.handleGreet(channel, consumerInfo1, 30, null);
      Thread.sleep(3000);
      Assert.assertTrue(consumerWorker1.getWaitAckMessages().get(channel).size()==30);
      
      
      makeMessage();
      consumerWorkerManager.handleAck(channel, consumerInfo1, 50L, ACKHandlerType.SEND_MESSAGE);
      Thread.sleep(3000);
      Assert.assertTrue(messageSet2.size() ==1);
      for(SwallowMessage message : messageSet2){
         Assert.assertTrue(message.getMessageId().equals(30L));
      }
      Assert.assertTrue(consumerWorker1.getWaitAckMessages().get(channel).size()==31);
      
      
      consumerWorkerManager.handleChannelDisconnect(channel,consumerInfo1);
      Thread.sleep(3000);
      Assert.assertTrue(consumerWorker1.getCachedMessages().size() == 31);
      Assert.assertTrue(consumerWorker1.getWaitAckMessages().get(channel) == null);
   }
   
      
   
//   @Test
//   public void testHandleAck() throws InterruptedException {
//      makeMessages();
//      Channel channel = mock(Channel.class);
//      when(channel.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8081));
//      when(channel.isConnected()).thenReturn(true);
//      when(channel.write(argThat(new Matcher<Object>() {
//         @Override
//         public void describeTo(Description arg0) {
//            // TODO Auto-generated method stub            
//         }
//         @Override
//         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
//            // TODO Auto-generated method stub           
//         }
//         @Override
//         public boolean matches(Object arg0) {
//            messageSet2.add(((PktMessage)arg0).getContent());
//            return true;
//         }
//      }))).thenReturn(null);
//      ConsumerId consumerId2 = new ConsumerId("zhangyu2", Destination.topic("zhangyuxx2"));
//      ConsumerInfo consumerInfo2 = new ConsumerInfo(consumerId2, ConsumerType.AT_MOST);      
//      consumerWorker2 = (ConsumerWorkerImpl)consumerWorkerManager.findOrCreateConsumerWorker(consumerInfo2, null);
//      consumerWorker2.setMessageQueue(messageQueue);
//      consumerWorkerManager.handleAck(channel, consumerInfo2, 50L, ACKHandlerType.SEND_MESSAGE);
//      Thread.sleep(5000);
//      Assert.assertTrue(messageSet2.size() ==1);
//      for(SwallowMessage message : messageSet2){
//         Assert.assertTrue(message.getMessageId().equals(0L));
//      }
//      
//   }
   
//      private void getConsumerWorkerImpl(){
//
//         consumerWorker = (ConsumerWorkerImpl)consumerWorkerManager.findOrCreateConsumerWorker(consumerInfo, null);
//          
//      }
//      
//      @Test
//      public void testGetMessageIdOfTailMessage(){
//         getConsumerWorkerImpl();
//         Channel channel = mock(AbstractChannel.class);
//         long maxMessageId;
//         maxMessageId = consumerWorker.getMessageIdOfTailMessage("aaxx","zhangyu",channel);
//         Assert.assertTrue(maxMessageId>0);
//         maxMessageId = consumerWorker.getMessageIdOfTailMessage("xx","zhangyu",channel);
//         Assert.assertTrue(maxMessageId>0);
//         maxMessageId = consumerWorker.getMessageIdOfTailMessage("xx","meiyoude",channel);
//         Assert.assertTrue(maxMessageId>0);
//      }

}
