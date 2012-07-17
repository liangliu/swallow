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
import com.dianping.swallow.consumer.impl.ConsumerImpl;

/**
 * <em>Internal-use-only</em> used by Swallow. <strong>DO NOT</strong> access
 * this class outside of Swallow.
 * 
 * @author zhang.yu
 */
public class MessageClientHandler extends SimpleChannelUpstreamHandler {

   private static final Logger LOG      = LoggerFactory.getLogger(MessageClientHandler.class);

   //TODO 统一cat_type
   private static final String CAT_TYPE = "swallow";
   private static final String CAT_NAME = "consumeMessage";

   private ConsumerImpl        cClient;

   private PktConsumerMessage  consumermessage;

   private ExecutorService     service;

   public MessageClientHandler(ConsumerImpl cClient) {
      this.cClient = cClient;
      service = Executors.newFixedThreadPool(cClient.getConfig().getThreadPoolSize());

   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {

      consumermessage = new PktConsumerMessage(ConsumerMessageType.GREET, cClient.getConsumerId(), cClient.getDest(),
            cClient.getConfig().getConsumerType(), cClient.getConfig().getThreadPoolSize(), cClient.getConfig()
                  .getMessageFilter());

      e.getChannel().write(consumermessage);
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      Runnable task = new Runnable() {

         @Override
         public void run() {
            SwallowMessage swallowMessage = (SwallowMessage) ((PktMessage) e.getMessage()).getContent();
            Long messageId = swallowMessage.getMessageId();

            consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, messageId, cClient.isClosed());

            //使用CAT监控处理消息的时间
            Transaction t = Cat.getProducer().newTransaction(CAT_TYPE, CAT_NAME);
            Event event = Cat.getProducer().newEvent(CAT_TYPE, CAT_NAME);
            event.addData(swallowMessage.toString());

            //处理消息
            //如果是压缩后的消息，则进行解压缩
            try {
               if (swallowMessage.getInternalProperties() != null) {
                  if ("gzip".equals(swallowMessage.getInternalProperties().get("compress"))) {
                     swallowMessage.setContent(ZipUtil.unzip(swallowMessage.getContent()));
                  }
               }
               try {
                  cClient.getListener().onMessage(swallowMessage);
               } catch (Exception e) {
                  LOG.error("exception in MessageListener", e);
               }
               event.setStatus(Event.SUCCESS);
            } catch (IOException e) {
               LOG.error("can not uncompress message with messageId " + messageId, e);
               t.setStatus(e);
            }

            event.complete();
            t.setStatus(Transaction.SUCCESS);//TODO 使用哪个setStatus
            t.complete();
            try {
               e.getChannel().write(consumermessage);
            } catch (RuntimeException e) {
               LOG.warn("write to swallowC error.", e);
            }

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
