package com.dianping.swallow.producerserver.impl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.dao.impl.mongodb.MessageDAOImpl;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.producerserver.util.TextHandler;

public class ProducerServerTextHandler extends SimpleChannelUpstreamHandler {
   private final MessageDAOImpl messageDAO;

   private static final Logger  logger = Logger.getLogger(ProducerServerTextHandler.class);

   public ProducerServerTextHandler(MessageDAOImpl messageDAO) {
      this.messageDAO = messageDAO;
   }

   @Override
   public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
      if (e instanceof ChannelStateEvent) {
         logger.info(e.toString());
      }
      super.handleUpstream(ctx, e);
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      String jsonStr = (String) e.getMessage();
      PktTextMessage pkt = TextHandler.changeTextToPacket(e.getChannel().getRemoteAddress(), jsonStr);
      ObjectMapper mapper = new ObjectMapper();
      TextACK textAck = new TextACK();
      textAck.setOK(true);
      if (pkt == null) {
         logger.error("TextMessage { " + e.getChannel().getRemoteAddress() + ": " + jsonStr + "} [Wrong format.]");
         textAck.setOK(false);
         textAck.setReason("Wrong json string.");
         jsonStr = mapper.writeValueAsString(textAck);
         e.getChannel().write(jsonStr);
      } else {
         if (!pkt.isACK())
            return;
         try {
            messageDAO.saveMessage(pkt.getTopicName(), pkt.getMessage());
         } catch (Exception e1) {
            logger.log(Level.ERROR, "[TextHandler]:[Save Message Failed.]");
            textAck.setOK(false);
            textAck.setReason("Can not save message.");
         }
         textAck.setSha1(pkt.getMessage().getSha1());
         jsonStr = mapper.writeValueAsString(textAck);
         e.getChannel().write(jsonStr);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      logger.log(Level.WARN, "Unexpected exception from downstream.", e.getCause());
      e.getChannel().close();
   }
}
