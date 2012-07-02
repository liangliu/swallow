package com.dianping.swallow.consumer.netty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.consumer.ConsumerMessageType;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.consumer.ConsumerClient;

public class MessageClientHandler extends SimpleChannelUpstreamHandler {

   private static final Logger LOG = LoggerFactory.getLogger(MessageClientHandler.class);
   
   private ConsumerClient      cClient;

   private PktConsumerMessage  consumermessage;

   private ExecutorService     service;

   public MessageClientHandler(ConsumerClient cClient) {
      this.cClient = cClient;
      service = Executors.newFixedThreadPool(cClient.getThreadCount());

   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {

      consumermessage = new PktConsumerMessage(ConsumerMessageType.GREET, cClient.getConsumerId(), cClient.getDest(),
            cClient.getConsumerType(), cClient.getThreadCount());
      e.getChannel().write(consumermessage);
      //如果是多线程，则除了greet消息外，仍需发送threadCount-1次ACK。
      if (cClient.getThreadCount() > 1) {
         int threadCount = cClient.getThreadCount();
         for (int i = 1; i < threadCount; i++) {
            consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, null, cClient.getNeedClose());
            e.getChannel().write(consumermessage);
         }
      }
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      Runnable task = new Runnable() {

         @Override
         public void run() {
            SwallowMessage swallowMessage = (SwallowMessage) ((PktMessage) e.getMessage()).getContent();
            Long messageId = swallowMessage.getMessageId();
            PktConsumerMessage consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, messageId,
                  cClient.getNeedClose());
            try{
               cClient.getListener().onMessage(swallowMessage);
            } catch(Exception e1){
               LOG.error("deal with message error!",e1);
            }
            
            e.getChannel().write(consumermessage);
         }
      };

      service.submit(task);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      // Close the connection when an exception is raised.
      
      LOG.error("exception caught, disconnect from swallowC", e.getCause());
      e.getChannel().close();
   }
}
