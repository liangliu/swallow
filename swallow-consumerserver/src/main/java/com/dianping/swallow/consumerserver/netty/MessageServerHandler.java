package com.dianping.swallow.consumerserver.netty;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.consumer.ACKHandlerType;
import com.dianping.swallow.common.consumer.ConsumerMessageType;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.consumerserver.worker.ConsumerId;
import com.dianping.swallow.consumerserver.worker.ConsumerInfo;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

@SuppressWarnings("deprecation")
@ChannelPipelineCoverage("all")
public class MessageServerHandler extends SimpleChannelUpstreamHandler {

   private ConsumerWorkerManager workerManager;

   private ConsumerId            consumerId;

   private ConsumerInfo          consumerInfo;

   private int                   clientThreadCount = 1;

   private boolean               readyClose        = Boolean.FALSE;

   public MessageServerHandler(ConsumerWorkerManager workerManager) {
      this.workerManager = workerManager;
   }

   //TODO log4j
   private static final Logger logger = Logger.getLogger(MessageServerHandler.class.getName());

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {

   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

      //收到PktConsumerACK，按照原流程解析
      Channel channel = e.getChannel();
      if (e.getMessage() instanceof PktConsumerMessage) {
         PktConsumerMessage consumerPacket = (PktConsumerMessage) e.getMessage();
         if (ConsumerMessageType.GREET.equals(consumerPacket.getType())) {
            clientThreadCount = consumerPacket.getThreadCount();
            consumerId = new ConsumerId(consumerPacket.getConsumerId(), consumerPacket.getDest());
            consumerInfo = new ConsumerInfo(consumerId, consumerPacket.getConsumerType());
            workerManager.handleGreet(channel, consumerInfo);
         } else {
            if (consumerPacket.getNeedClose() || readyClose) {
               clientThreadCount--;
               readyClose = Boolean.TRUE;
            }
            if (readyClose && clientThreadCount == 0) {
               workerManager.handleAck(channel, consumerInfo, consumerPacket.getMessageId(),
                     ACKHandlerType.CLOSE_CHANNEL);
            } else if (readyClose && clientThreadCount > 0) {
               workerManager.handleAck(channel, consumerInfo, consumerPacket.getMessageId(), ACKHandlerType.NO_SEND);
            } else if (!readyClose) {
               workerManager.handleAck(channel, consumerInfo, consumerPacket.getMessageId(),
                     ACKHandlerType.SEND_MESSAGE);
            }
         }

      } else {
         //TODO 记日志
      }

   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      logger.log(Level.WARNING, "客户端断开连接！");
      //只有IOException的时候才需要处理。
      if (e.getCause() instanceof IOException) {
         Channel channel = e.getChannel();
         workerManager.handleChannelDisconnect(channel, consumerInfo);
         channel.close();
      }
   }
}
