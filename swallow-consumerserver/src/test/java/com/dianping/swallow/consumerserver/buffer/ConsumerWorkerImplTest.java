package com.dianping.swallow.consumerserver.buffer;

import static org.mockito.Mockito.mock;

import org.jboss.netty.channel.AbstractChannel;
import org.jboss.netty.channel.Channel;
import org.junit.Assert;
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


public class ConsumerWorkerImplTest extends AbstractDAOImplTest{
      
   @Autowired
   private AckDAO                                 ackDAO;
   @Autowired
   private ConsumerWorkerManager consumerWorkerManager;
   private ConsumerWorkerImpl consumerWorker;
   
   
//   private void getConsumerWorkerImpl(){
//      ConsumerId consumerId = new ConsumerId("zhangyu", Destination.topic("aaxx"));
//      ConsumerInfo consumerInfo = new ConsumerInfo(consumerId, ConsumerType.AT_LEAST);
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
