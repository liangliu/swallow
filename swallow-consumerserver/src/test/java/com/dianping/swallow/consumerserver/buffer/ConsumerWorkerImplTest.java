package com.dianping.swallow.consumerserver.buffer;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jboss.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.internal.dao.AckDAO;
import com.dianping.swallow.common.internal.dao.MessageDAO;
import com.dianping.swallow.common.internal.dao.impl.mongodb.AbstractDAOImplTest;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumerserver.worker.ConsumerId;
import com.dianping.swallow.consumerserver.worker.ConsumerInfo;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class ConsumerWorkerImplTest extends AbstractDAOImplTest {

   @Autowired
   private ConsumerWorkerManager consumerWorkerManager;
   private Set<SwallowMessage>      messageSetChecker = new HashSet<SwallowMessage>();
   
   private Channel channel;
   
   private void makeMessages(BlockingQueue<Message> messageQueue){
      for(long i=0;i<50;i++ ){
         SwallowMessage message = new SwallowMessage();
         message.setMessageId(i);
         //messageSetChecker.add(message);
         messageQueue.add(message);
      }
      
   }
   private Boolean check(int i){
      if(messageSetChecker.size() != i){
         return false;
      }
      return true;
   }
//   private void makeMessage(BlockingQueue<Message> messageQueue){
//      
//         SwallowMessage message = new SwallowMessage();
//         message.setMessageId(123456L);
//         messageSetChecker= new HashSet<SwallowMessage>();
//         messageQueue.add(message);
//      
//      
//   }
   private void mockDao(){
      SwallowBuffer swallowBuffer = mock(SwallowBuffer.class);
      CloseableBlockingQueue<Message> messageQueue = new MockedCloseableBlockingQueue<Message>();
      
      makeMessages(messageQueue);
//      when(swallowBuffer.createMessageQueue("xx", "dp1", 123456L,
//            null)).thenReturn(messageQueue);
//      when(swallowBuffer.createMessageQueue("xx", "dp11", 234567L,
//            null)).thenReturn(messageQueue);
      when(swallowBuffer.createMessageQueue(Matchers.anyString(), Matchers.anyString(), Matchers.anyLong(),
            (MessageFilter)Matchers.anyObject())).thenReturn(messageQueue);
      AckDAO ackDAO = mock(AckDAO.class);
      //doReturn(print()).when(ackDAO).add("", "", 123L, "");
      MessageDAO messageDAO = mock(MessageDAO.class);
      when(ackDAO.getMaxMessageId("xx", "dp1")).thenReturn(123456L);
      when(ackDAO.getMaxMessageId("xxx", "dp1")).thenReturn(null);
      when(ackDAO.getMaxMessageId("xx", "dp11")).thenReturn(null);
      when(messageDAO.getMaxMessageId("xx")).thenReturn(234567L);
      when(messageDAO.getMaxMessageId("xxx")).thenReturn(null);
      
      consumerWorkerManager.setAckDAO(ackDAO);
      consumerWorkerManager.setMessageDAO(messageDAO);
      consumerWorkerManager.setSwallowBuffer(swallowBuffer);
   }
   
//   String print(){
//      System.out.println("hello,world");
//      return null;
//   }
   
   private void mockChannel(){      
      channel = mock(Channel.class);
      when(channel.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8081));
      when(channel.isConnected()).thenReturn(true);
      when(channel.write(argThat(new Matcher<Object>() {
         @Override
         public void describeTo(Description arg0) {
                       
         }
         @Override
         public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
                    
         }
         @Override
         public boolean matches(Object arg0) {
            messageSetChecker.add(((PktMessage)arg0).getContent());
            return true;
         }
      }))).thenReturn(null);
   }
   
   @Test
   public void testHandleGreet_topicFirst() throws InterruptedException{
      mockChannel();
      mockDao();
      ConsumerId consumerId3 = new ConsumerId("dp1", Destination.topic("xxx"));
      ConsumerInfo consumerInfo3 = new ConsumerInfo(consumerId3, ConsumerType.AT_MOST_ONCE);      
      consumerWorkerManager.handleGreet(channel, consumerInfo3, 50, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(50));
   }
   @Test
   public void testHandleGreet_consumerFirst() throws InterruptedException{
      mockChannel();
      mockDao();
    ConsumerId consumerId2 = new ConsumerId("dp11", Destination.topic("xx"));
    ConsumerInfo consumerInfo2 = new ConsumerInfo(consumerId2, ConsumerType.AT_MOST_ONCE);      
    consumerWorkerManager.handleGreet(channel, consumerInfo2, 50, null);
    Thread.sleep(3000);
    Assert.assertTrue(check(50));
   }
   
   @Test
   public void testHandleGreet() throws InterruptedException {
           
      mockChannel();
      mockDao();
      
      ConsumerId consumerId1 = new ConsumerId("dp1", Destination.topic("xx"));
      ConsumerInfo consumerInfo1 = new ConsumerInfo(consumerId1, ConsumerType.AT_MOST_ONCE);      
      consumerWorkerManager.handleGreet(channel, consumerInfo1, 50, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(50));
      

      
//
//      consumerWorkerManager.handleAck(channel, consumerInfo2, 50L, ACKHandlerType.SEND_MESSAGE);
//      Thread.sleep(3000);
//      Assert.assertTrue(messageSetChecker.size() ==1);
//      for(SwallowMessage message : messageSetChecker){
//         Assert.assertTrue(message.getMessageId().equals(123456L));
//      }
//      
//
//      ConsumerId consumerId1 = new ConsumerId("zhangyu1", Destination.topic("zhangyuxx1"));
//      ConsumerInfo consumerInfo1 = new ConsumerInfo(consumerId1, ConsumerType.AT_LEAST_ONCE);      
//      consumerWorker1 = (ConsumerWorkerImpl)consumerWorkerManager.findOrCreateConsumerWorker(consumerInfo1, null);
//      consumerWorker1.setMessageQueue(messageQueue);
//      consumerWorkerManager.handleGreet(channel, consumerInfo1, 30, null);
//      Thread.sleep(3000);
//      Assert.assertTrue(consumerWorker1.getWaitAckMessages().get(channel).size()==30);
//      
//      
//
//      consumerWorkerManager.handleAck(channel, consumerInfo1, 50L, ACKHandlerType.SEND_MESSAGE);
//      Thread.sleep(3000);
//      Assert.assertTrue(messageSetChecker.size() ==1);
//      for(SwallowMessage message : messageSetChecker){
//         Assert.assertTrue(message.getMessageId().equals(30L));
//      }
//      Assert.assertTrue(consumerWorker1.getWaitAckMessages().get(channel).size()==31);
//      
//      
//      consumerWorkerManager.handleChannelDisconnect(channel,consumerInfo1);
//      Thread.sleep(3000);
//      Assert.assertTrue(consumerWorker1.getCachedMessages().size() == 31);
//      Assert.assertTrue(consumerWorker1.getWaitAckMessages().get(channel) == null);
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
   class MockedCloseableBlockingQueue<E> extends LinkedBlockingQueue<E> implements CloseableBlockingQueue<E> {
      private static final long serialVersionUID = 1L;

      @Override
      public void close() {
      }

      @Override
      public void isClosed() {
      }
   }


}
