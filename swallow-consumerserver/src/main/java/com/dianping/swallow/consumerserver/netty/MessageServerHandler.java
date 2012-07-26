package com.dianping.swallow.consumerserver.netty;

import java.util.UUID;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.internal.consumer.ACKHandlerType;
import com.dianping.swallow.common.internal.consumer.ConsumerMessageType;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.util.NameCheckUtil;
import com.dianping.swallow.consumerserver.impl.MongoHeartbeater;
import com.dianping.swallow.consumerserver.worker.ConsumerId;
import com.dianping.swallow.consumerserver.worker.ConsumerInfo;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class MessageServerHandler extends SimpleChannelUpstreamHandler {

   private static final Logger   LOG          = LoggerFactory.getLogger(MongoHeartbeater.class);

   private static ChannelGroup   channelGroup = new DefaultChannelGroup();

   private ConsumerWorkerManager workerManager;

   private ConsumerId            consumerId;

   private ConsumerInfo          consumerInfo;

   private int                   clientThreadCount;

   private boolean               readyClose   = Boolean.FALSE;

   public MessageServerHandler(ConsumerWorkerManager workerManager) {
      this.workerManager = workerManager;
   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      LOG.info(e.getChannel().getRemoteAddress() + " connected!");
      channelGroup.add(e.getChannel());
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

      //收到PktConsumerACK，按照原流程解析
      final Channel channel = e.getChannel();
      if (e.getMessage() instanceof PktConsumerMessage) {
         PktConsumerMessage consumerPacket = (PktConsumerMessage) e.getMessage();
         if (ConsumerMessageType.GREET.equals(consumerPacket.getType())) {
            if (!NameCheckUtil.isTopicNameValid(consumerPacket.getDest().getName())) {
               LOG.error("TopicName inValid from " + channel.getRemoteAddress());
               channel.close();
               return;
            }
            clientThreadCount = consumerPacket.getThreadCount();
            if (clientThreadCount > workerManager.getConfigManager().getMaxClientThreadCount()) {
               LOG.warn(channel.getRemoteAddress() + " with " + consumerInfo
                     + "clientThreadCount greater than MaxClientThreadCount("
                     + workerManager.getConfigManager().getMaxClientThreadCount() + ")");
               clientThreadCount = workerManager.getConfigManager().getMaxClientThreadCount();
            }
            String strConsumerId = consumerPacket.getConsumerId();
            if (strConsumerId == null || strConsumerId.trim().length() == 0) {
               consumerId = new ConsumerId(fakeCid(), consumerPacket.getDest());
               consumerInfo = new ConsumerInfo(consumerId, ConsumerType.NON_DURABLE);
            } else {
               if (!NameCheckUtil.isConsumerIdValid(consumerPacket.getConsumerId())) {
                  LOG.error("ConsumerId inValid from " + channel.getRemoteAddress());
                  channel.close();
                  return;
               }
               consumerId = new ConsumerId(strConsumerId, consumerPacket.getDest());
               consumerInfo = new ConsumerInfo(consumerId, consumerPacket.getConsumerType());
            }
            LOG.info("received greet from " + e.getChannel().getRemoteAddress() + " with " + consumerInfo);
            workerManager.handleGreet(channel, consumerInfo, clientThreadCount, consumerPacket.getMessageFilter());
         }
         if (ConsumerMessageType.ACK.equals(consumerPacket.getType())) {
            if (consumerPacket.getNeedClose() || readyClose) {
               //第一次接到channel的close命令后,server启一个后台线程,当一定时间后channel仍未关闭,则强制关闭.
               if (!readyClose) {
                  Thread thread = workerManager.getThreadFactory().newThread(new Runnable() {

                     @Override
                     public void run() {
                        try {
                           Thread.sleep(workerManager.getConfigManager().getCloseChannelMaxWaitingTime());
                        } catch (InterruptedException e) {
                           LOG.error("CloseChannelThread InterruptedException", e);
                        }
                        // channel.getRemoteAddress() 在channel断开后,不会抛异常
                        LOG.info("CloseChannelMaxWaitingTime reached, close channel " + channel.getRemoteAddress()
                              + " with " + consumerInfo);
                        channel.close();
                        workerManager.handleChannelDisconnect(channel, consumerInfo);
                     }
                  }, consumerInfo.toString() + "-CloseChannelThread-");
                  thread.setDaemon(true);
                  thread.start();
               }
               clientThreadCount--;
               readyClose = Boolean.TRUE;
            }
            ACKHandlerType handlerType = null;
            if (readyClose && clientThreadCount == 0) {
               handlerType = ACKHandlerType.CLOSE_CHANNEL;
            } else if (readyClose && clientThreadCount > 0) {
               handlerType = ACKHandlerType.NO_SEND;
            } else if (!readyClose) {
               handlerType = ACKHandlerType.SEND_MESSAGE;
            }
            workerManager.handleAck(channel, consumerInfo, consumerPacket.getMessageId(), handlerType);
         }
      } else {
         LOG.warn("the received message is not PktConsumerMessage");
      }

   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      removeChannel(e);
      LOG.error("Client disconnected from swallowC!", e.getCause());
      e.getChannel().close();

   }

   @Override
   public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      removeChannel(e);
      super.channelDisconnected(ctx, e);
   }

   @Override
   public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      removeChannel(e);
      super.channelClosed(ctx, e);
   }

   private void removeChannel(ChannelEvent e) {
      channelGroup.remove(e.getChannel());
      Channel channel = e.getChannel();
      workerManager.handleChannelDisconnect(channel, consumerInfo);
   }

   public static ChannelGroup getChannelGroup() {
      return channelGroup;
   }

   //生成唯一consumerId
   private String fakeCid() {
      return UUID.randomUUID().toString();
   }

}
