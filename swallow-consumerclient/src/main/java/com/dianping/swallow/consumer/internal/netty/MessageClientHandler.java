package com.dianping.swallow.consumer.internal.netty;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.swallow.common.internal.consumer.ConsumerMessageType;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.threadfactory.MQThreadFactory;
import com.dianping.swallow.common.internal.util.ZipUtil;
import com.dianping.swallow.consumer.internal.ConsumerImpl;

/**
 * <em>Internal-use-only</em> used by Swallow. <strong>DO NOT</strong> access
 * this class outside of Swallow.
 * 
 * @author zhang.yu
 */
public class MessageClientHandler extends SimpleChannelUpstreamHandler {

   private static final Logger LOG = LoggerFactory.getLogger(MessageClientHandler.class);

   private ConsumerImpl        consumer;

   private ExecutorService     service;

   public MessageClientHandler(ConsumerImpl consumer) {
      this.consumer = consumer;
      service = Executors.newFixedThreadPool(consumer.getConfig().getThreadPoolSize(), new MQThreadFactory(
            "swallow-consumer-client-"));
   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      PktConsumerMessage consumermessage = new PktConsumerMessage(ConsumerMessageType.GREET, consumer.getConsumerId(),
            consumer.getDest(), consumer.getConfig().getConsumerType(), consumer.getConfig().getThreadPoolSize(),
            consumer.getConfig().getMessageFilter());
      e.getChannel().write(consumermessage);
   }

   @Override
   public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      super.channelDisconnected(ctx, e);
      LOG.info("channel(remoteAddress=" + e.getChannel().getRemoteAddress() + ") disconnected");
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      //记录收到消息，并且记录发来消息的server的地址
      if (LOG.isDebugEnabled()) {
         LOG.debug("messageReceived from " + ctx.getChannel().getRemoteAddress());
      }

      Runnable task = new Runnable() {

         @Override
         public void run() {
            SwallowMessage swallowMessage = ((PktMessage) e.getMessage()).getContent();
            
            String catParentID = ((PktMessage)e.getMessage()).getCatEventID();
            
            Long messageId = swallowMessage.getMessageId();

            PktConsumerMessage consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, messageId,
                  consumer.isClosed());

            //使用CAT监控处理消息的时间
            try {
               MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
               if (tree != null) {
                  tree.setParentMessageId(catParentID);
               }
            } catch (Exception e) {
               LOG.warn("error get Cat tree", e);
            }
            
            Transaction t = Cat.getProducer().newTransaction("MessageConsumed", consumer.getDest().getName() + ":" + consumer.getConsumerId());
            Event event = Cat.getProducer().newEvent("Message", "payload");
            event.addData("mid", swallowMessage.getMessageId());
            event.addData("sha1", swallowMessage.getSha1());

            //处理消息
            //如果是压缩后的消息，则进行解压缩
            try {
               if (swallowMessage.getInternalProperties() != null) {
                  if ("gzip".equals(swallowMessage.getInternalProperties().get("compress"))) {
                     swallowMessage.setContent(ZipUtil.unzip(swallowMessage.getContent()));
                  }
               }
               try {
                  consumer.getListener().onMessage(swallowMessage);
               } catch (Exception e) {
                  LOG.info("exception in MessageListener", e);
               }
               event.setStatus(Message.SUCCESS);
               t.setStatus(Message.SUCCESS);
            } catch (IOException e) {
               LOG.error("can not uncompress message with messageId " + messageId, e);
               event.setStatus(e);
               t.setStatus(e);
            } finally {
               event.complete();
               t.complete();
            }

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
      Channel channel = e.getChannel();
      LOG.error("error from channel(remoteAddress=" + channel.getRemoteAddress() + ")", e.getCause());
      channel.close();
   }
}
