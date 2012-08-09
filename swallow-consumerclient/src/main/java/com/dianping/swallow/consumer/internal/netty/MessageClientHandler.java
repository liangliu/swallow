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
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.swallow.common.internal.consumer.ConsumerMessageType;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.threadfactory.DefaultPullStrategy;
import com.dianping.swallow.common.internal.threadfactory.MQThreadFactory;
import com.dianping.swallow.common.internal.util.ZipUtil;
import com.dianping.swallow.consumer.BackoutMessageException;
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
         LOG.debug("messageReceived from " + e.getChannel().getRemoteAddress());
      }

      Runnable task = new Runnable() {

         @Override
         public void run() {
            SwallowMessage swallowMessage = ((PktMessage) e.getMessage()).getContent();

            Long messageId = swallowMessage.getMessageId();

            PktConsumerMessage consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, messageId,
                  consumer.isClosed());

            //使用CAT监控处理消息的时间

            Transaction consumerClientTransaction = Cat.getProducer().newTransaction("MessageConsumed",
                  consumer.getDest().getName() + ":" + consumer.getConsumerId());
            consumerClientTransaction.addData("mid", swallowMessage.getMessageId());
            consumerClientTransaction.addData("sha1", swallowMessage.getSha1());

            try {
               MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
               String catParentID = ((PktMessage) e.getMessage()).getCatEventID();
               tree.setMessageId(catParentID);
            } catch (Exception e) {
            }

            //处理消息
            //如果是压缩后的消息，则进行解压缩
            try {
               if (swallowMessage.getInternalProperties() != null) {
                  if ("gzip".equals(swallowMessage.getInternalProperties().get("compress"))) {
                     swallowMessage.setContent(ZipUtil.unzip(swallowMessage.getContent()));
                  }
               }
               try {
                  DefaultPullStrategy pullStrategy = new DefaultPullStrategy(MessageClientHandler.this.consumer
                        .getConfig().getDelayBaseOnBackoutMessageException(), MessageClientHandler.this.consumer
                        .getConfig().getDelayUpperboundOnBackoutMessageException());
                  int retryCount = 0;
                  boolean success = false;
                  while (!success
                        && retryCount <= MessageClientHandler.this.consumer.getConfig()
                              .getRetryCountOnBackoutMessageException()) {
                     try {
                        consumer.getListener().onMessage(swallowMessage);
                        success = true;
                     } catch (BackoutMessageException e) {
                        retryCount++;
                        if (retryCount <= MessageClientHandler.this.consumer.getConfig()
                              .getRetryCountOnBackoutMessageException()) {
                           LOG.error(
                                 "BackoutMessageException occur on onMessage(), onMessage() will be retryed soon [retryCount="
                                       + retryCount + "]. ", e);
                           pullStrategy.fail(true);
                        } else {
                           LOG.error("BackoutMessageException occur on onMessage(), onMessage() failed.", e);
                        }
                     }
                  }
               } catch (Exception e) {
                  LOG.info("exception in MessageListener", e);
               }
               consumerClientTransaction.setStatus(Message.SUCCESS);
            } catch (IOException e) {
               LOG.error("can not uncompress message with messageId " + messageId, e);
               consumerClientTransaction.setStatus(e);
               Cat.getProducer().logError(e);
            } finally {
               consumerClientTransaction.complete();
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
