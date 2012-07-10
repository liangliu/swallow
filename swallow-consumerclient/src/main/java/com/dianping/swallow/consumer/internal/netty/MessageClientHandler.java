package com.dianping.swallow.consumer.internal.netty;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.swallow.common.internal.consumer.ConsumerMessageType;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.util.ZipUtil;
import com.dianping.swallow.consumer.impl.ConsumerClientImpl;

/**
 * <em>Internal-use-only</em> used by Swallow. <strong>DO NOT</strong> access
 * this class outside of Swallow.
 * 
 * @author zhang.yu
 */
public class MessageClientHandler extends SimpleChannelUpstreamHandler {

   private static final Logger LOG      = LoggerFactory.getLogger(MessageClientHandler.class);

   private static final String CAT_TYPE = "swallow";
   private static final String CAT_NAME = "consumeMessage";

   private ConsumerClientImpl      cClient;

   private PktConsumerMessage  consumermessage;

   private ExecutorService     service;

   public MessageClientHandler(ConsumerClientImpl cClient) {
      this.cClient = cClient;
      service = Executors.newFixedThreadPool(cClient.getThreadCount());

   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {

      consumermessage = new PktConsumerMessage(ConsumerMessageType.GREET, cClient.getConsumerId(), cClient.getDest(),
            cClient.getConsumerType(), cClient.getThreadCount(), cClient.getNeededMessageType());

      e.getChannel().write(consumermessage);
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      Runnable task = new Runnable() {

         @Override
         public void run() {
            SwallowMessage swallowMessage = (SwallowMessage) ((PktMessage) e.getMessage()).getContent();
            Long messageId = swallowMessage.getMessageId();

            consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, messageId, cClient.getNeedClose());

            //使用CAT监控处理消息的时间
            Transaction t = Cat.getProducer().newTransaction(CAT_TYPE, CAT_NAME);
            try {
               Event event = Cat.getProducer().newEvent(CAT_TYPE, CAT_NAME);
               event.addData(swallowMessage.toString());

               //处理消息
             //如果是压缩后的消息，则进行解压缩
               if (swallowMessage.getInternalProperties() != null) {
                  if ("gzip".equals(swallowMessage.getInternalProperties().get("compress"))) {
                     try {
                        swallowMessage.setContent(ZipUtil.unzip(swallowMessage.getContent()));
                     } catch (IOException e) {
                        LOG.error("ZipUtil.unzip error!", e);
                     }
                  }
               }
               cClient.getListener().onMessage(swallowMessage);

               event.setStatus(Event.SUCCESS);
               event.complete();
               t.setStatus(Transaction.SUCCESS);
            } catch (Exception e) {
               LOG.error("deal with message error!", e);
               t.setStatus(e);
            } finally {
               t.complete();
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
