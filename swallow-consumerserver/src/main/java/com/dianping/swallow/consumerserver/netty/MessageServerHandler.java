package com.dianping.swallow.consumerserver.netty;

import java.io.IOException;
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

   private static final Logger LOG        = LoggerFactory.getLogger(MongoHeartbeater.class);
   
   private static ChannelGroup channelGroup = new DefaultChannelGroup();
   
   private ConsumerWorkerManager workerManager;

   private ConsumerId            consumerId;

   private ConsumerInfo          consumerInfo;

   private int                   clientThreadCount;

   private boolean               readyClose        = Boolean.FALSE;

   public MessageServerHandler(ConsumerWorkerManager workerManager) {
      this.workerManager = workerManager;
   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      LOG.info(e.getChannel().getRemoteAddress() + " connected!");
      channelGroup.add(e.getChannel());
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {

      //收到PktConsumerACK，按照原流程解析
      final Channel channel = e.getChannel();
      if (e.getMessage() instanceof PktConsumerMessage) {
         PktConsumerMessage consumerPacket = (PktConsumerMessage) e.getMessage();
         if (!NameCheckUtil.isTopicNameValid(consumerPacket.getDest().getName())) {
            LOG.error("TopicName inValid from " + channel.getRemoteAddress());
         }
         if (ConsumerMessageType.GREET.equals(consumerPacket.getType())) {
            clientThreadCount = consumerPacket.getThreadCount();            
            if(consumerPacket.getConsumerId() == null){
               consumerId = new ConsumerId(fakeCid(), consumerPacket.getDest());
               consumerInfo = new ConsumerInfo(consumerId, ConsumerType.NON_DURABLE);
            }else{
               if(!NameCheckUtil.isConsumerIdValid(consumerPacket.getConsumerId())){
                  LOG.error("ConsumerId inValid from " + channel.getRemoteAddress());
               }
               consumerId = new ConsumerId(consumerPacket.getConsumerId(), consumerPacket.getDest());
               consumerInfo = new ConsumerInfo(consumerId, consumerPacket.getConsumerType());
            }     
            workerManager.handleGreet(channel, consumerInfo, clientThreadCount, consumerPacket.getMessageFilter());
         } 
         if(ConsumerMessageType.ACK.equals(consumerPacket.getType())){
            if (consumerPacket.getNeedClose() || readyClose) {
               //第一次接到channel的close命令后,server启一个后台线程,当一定时间后channel仍未关闭,则强制关闭.
               if(!readyClose){
                  Thread thread = workerManager.getThreadFactory().newThread(new Runnable() {

                     @Override
                     public void run() {
                        try {
                           Thread.sleep(workerManager.getConfigManager().getCloseChannelMaxWaitingTime());
                        } catch (InterruptedException e) {
                           LOG.error("CloseChannelThread InterruptedException", e);
                        }
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

   private void removeChannel(ChannelEvent e){
      channelGroup.remove(e.getChannel());
      Channel channel = e.getChannel();
      workerManager.handleChannelDisconnect(channel, consumerInfo);
   }
   
   public static ChannelGroup getChannelGroup() {
      return channelGroup;
   }
 //生成唯一consumerId
   private String fakeCid(){
      return UUID.randomUUID().toString();
   }

}
