package com.dianping.swallow.consumerserver.buffer;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jboss.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.internal.consumer.ACKHandlerType;
import com.dianping.swallow.common.internal.dao.AckDAO;
import com.dianping.swallow.common.internal.dao.MessageDAO;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.consumerserver.worker.ConsumerId;
import com.dianping.swallow.consumerserver.worker.ConsumerInfo;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerImpl;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class ConsumerWorkerImplTest extends AbstractTest {
   @Autowired
   private AckDAO ackDAO;
   @Autowired
   private MessageDAO messageDAO;
   @Autowired
   private ConsumerWorkerManager consumerWorkerManager;
   private Set<SwallowMessage>   messageSetChecker = new HashSet<SwallowMessage>();

   private Channel               channel;

   private void makeMessages(BlockingQueue<Message> messageQueue) {
      for (long i = 0; i < 50; i++) {
         SwallowMessage message = new SwallowMessage();
         message.setMessageId(i);
         messageQueue.add(message);
      }

   }

   private Boolean check(int i) {
      if (messageSetChecker.size() != i) {
         return false;
      }
      Iterator<SwallowMessage> it = messageSetChecker.iterator();
      while (it.hasNext()) {
         SwallowMessage message = it.next();
         if (message.getMessageId() >= i || message.getMessageId() < 0) {
            return false;
         }
      }
      return true;
   }

   @Before
   public void mockDao(){
      SwallowBuffer swallowBuffer = mock(SwallowBuffer.class);
      CloseableBlockingQueue<Message> messageQueue = new MockedCloseableBlockingQueue<Message>();

      makeMessages(messageQueue);
      when(
            swallowBuffer.createMessageQueue(Matchers.anyString(), Matchers.anyString(), Matchers.anyLong(),
                  (MessageFilter) Matchers.anyObject())).thenReturn(messageQueue);
//      AckDAO ackDAO = mock(AckDAO.class);
//      //doReturn(print()).when(ackDAO).add(Matchers.anyString(), Matchers.anyString(), Matchers.anyLong(), Matchers.anyString());
//      MessageDAO messageDAO = mock(MessageDAO.class);
//      when(ackDAO.getMaxMessageId("xx", "dp1")).thenReturn(123456L);
//      when(ackDAO.getMaxMessageId("xxx", "dp1")).thenReturn(null);
//      when(ackDAO.getMaxMessageId("xx", "dp11")).thenReturn(null);
//      doAnswer(new Answer<Object>() {
//         @Override
//         public String answer(InvocationOnMock invocation) throws Throwable {
//            System.out.println("RUN ackDAO.add()!");
//            return "hello";
//         }
//      }).when(ackDAO).add(Matchers.anyString(), Matchers.anyString(), Matchers.anyLong(), Matchers.anyString());
//      when(messageDAO.getMaxMessageId("xx")).thenReturn(234567L);
//      when(messageDAO.getMaxMessageId("xxx")).thenReturn(null);
      //准备数据
      ackDAO.add("xx", "dp1", 123456L, "127.0.0.1");
      SwallowMessage message = new SwallowMessage();
      message.setContent("this is a SwallowMessage");
      messageDAO.saveMessage("xx", message);

//      consumerWorkerManager.setAckDAO(ackDAO);
//      consumerWorkerManager.setMessageDAO(messageDAO);
      consumerWorkerManager.setSwallowBuffer(swallowBuffer);
   }

   @Before
   public void mockChannel(){      
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
            messageSetChecker.add(((PktMessage) arg0).getContent());
            return true;
         }
      }))).thenReturn(null);
   }

   /**
    * consumerType 为NON_DURABLE
    * 
    * @throws InterruptedException
    */
   @Test
   public void testHandleGreet_NON_DURABLE() throws InterruptedException {
//      mockChannel();
//      mockDao();
      ConsumerId consumerId2 = new ConsumerId("dp11", Destination.topic("xx"));
      ConsumerInfo consumerInfo2 = new ConsumerInfo(consumerId2, ConsumerType.NON_DURABLE);
      consumerWorkerManager.handleGreet(channel, consumerInfo2, 50, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(50));
   }

   /**
    * topic为xxx,xxx还没有消息
    * 
    * @throws InterruptedException
    */
   @Test
   public void testHandleGreet_topicFirst() throws InterruptedException {
//      mockChannel();
//      mockDao();
      ConsumerId consumerId3 = new ConsumerId("dp1", Destination.topic("xxx"));
      ConsumerInfo consumerInfo3 = new ConsumerInfo(consumerId3, ConsumerType.DURABLE_AT_MOST_ONCE);
      consumerWorkerManager.handleGreet(channel, consumerInfo3, 50, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(50));
   }

   /**
    * topic为xx，consumerId为dp11的第一次访问，但xx已经有消息产生了
    * 
    * @throws InterruptedException
    */
   @Test
   public void testHandleGreet_consumerFirst() throws InterruptedException {
//      mockChannel();
//      mockDao();
      ConsumerId consumerId2 = new ConsumerId("dp11", Destination.topic("xx"));
      ConsumerInfo consumerInfo2 = new ConsumerInfo(consumerId2, ConsumerType.DURABLE_AT_MOST_ONCE);
      consumerWorkerManager.handleGreet(channel, consumerInfo2, 50, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(50));
   }

   /**
    * topic为xx，consumerId为dp1。从greet到ack,再到最后disconnect
    * 
    * @throws InterruptedException
    */
   @Test
   public void testHandleGreet() throws InterruptedException {

//      mockChannel();
//      mockDao();

      ConsumerId consumerId1 = new ConsumerId("dp1", Destination.topic("xx"));
      ConsumerInfo consumerInfo1 = new ConsumerInfo(consumerId1, ConsumerType.DURABLE_AT_LEAST_ONCE);
      consumerWorkerManager.handleGreet(channel, consumerInfo1, 30, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(30));
      Assert.assertEquals(30,
            ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId1))
                  .getWaitAckMessages().get(channel).size());

      consumerWorkerManager.handleAck(channel, consumerInfo1, 20L, ACKHandlerType.SEND_MESSAGE);
      Thread.sleep(3000);
      Assert.assertEquals(30,
            ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId1))
                  .getWaitAckMessages().get(channel).size());
      Assert.assertTrue(check(31));
      Assert.assertEquals(0, ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker()
            .get(consumerId1)).getCachedMessages().size());

      consumerWorkerManager.handleAck(channel, consumerInfo1, 18L, ACKHandlerType.NO_SEND);
      Thread.sleep(2000);
      Assert.assertEquals(29,
            ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId1))
                  .getWaitAckMessages().get(channel).size());
      Assert.assertTrue(check(31));
      Assert.assertEquals(0, ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker()
            .get(consumerId1)).getCachedMessages().size());

      consumerWorkerManager.handleAck(channel, consumerInfo1, 19L, ACKHandlerType.CLOSE_CHANNEL);
      Thread.sleep(2000);
      Assert.assertEquals(0, ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker()
            .get(consumerId1)).getConnectedChannels().size());
      Assert.assertEquals(null,
            ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId1))
                  .getWaitAckMessages().get(channel));
      Assert.assertEquals(28,
            ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId1))
                  .getCachedMessages().size());

      consumerWorkerManager.handleGreet(channel, consumerInfo1, 30, null);
      Thread.sleep(3000);
      Assert.assertTrue(check(33));
      Assert.assertEquals(30,
            ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker().get(consumerId1))
                  .getWaitAckMessages().get(channel).size());
      Assert.assertEquals(0, ((ConsumerWorkerImpl) consumerWorkerManager.getConsumerId2ConsumerWorker()
            .get(consumerId1)).getCachedMessages().size());
   }

   static class MockedCloseableBlockingQueue<E> extends LinkedBlockingQueue<E> implements CloseableBlockingQueue<E> {
      private static final long serialVersionUID = 1L;

      @Override
      public void close() {
      }

      @Override
      public void isClosed() {
      }
   }

}
