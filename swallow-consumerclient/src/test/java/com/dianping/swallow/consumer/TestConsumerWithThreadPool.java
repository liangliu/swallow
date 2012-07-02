package com.dianping.swallow.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.MessageEvent;

import com.dianping.swallow.common.consumer.ConsumerMessageType;
import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktMessage;

public class TestConsumerWithThreadPool {

   //应该都是取自于lion
   public static String       swallowCAddress = "127.0.0.1:8081,127.0.0.1:8082";
   public static String       cid             = "zhangyu";
   public static Destination  dest            = Destination.topic("xx");
   public static ConsumerType consumerType    = ConsumerType.AT_LEAST;

   public static void main(String[] args) throws Exception {
      final ExecutorService service = Executors.newFixedThreadPool(10);
      //TODO 通过spring使用的example
      final ConsumerClient cClient = new ConsumerClient(cid, dest, swallowCAddress);
      cClient.setThreadCount(10);
      cClient.setConsumerType(consumerType);
      cClient.setListener(new MessageListener() {

         @Override
         public void onMessage(final MessageEvent e) {
            //用户得到SwallowMessage
            final SwallowMessage swallowMessage = (SwallowMessage)((PktMessage)e.getMessage()).getContent();
            Long messageId = swallowMessage.getMessageId();     
            final PktConsumerMessage consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK ,messageId, cClient.getNeedClose());
				Runnable run = new Runnable() {  
	                @Override  
	                public void run() {  
	                	System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
	                	e.getChannel().write(consumermessage);;
	                	try {
	    					Thread.sleep(500);
	    				} catch (InterruptedException e) {
	    					// TODO 使用LOG
	    					e.printStackTrace();
	    				}
	                }  
	            };
	            service.execute(run);  
         }

      });
      cClient.beginConnect();

   }
}
