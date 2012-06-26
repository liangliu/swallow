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

   /**
    * 构造函数
    * 
    * @param messageDAO
    */
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
      //获取json字符串
      String jsonStr = (String) e.getMessage();
      //处理json字符串，转换为PktTextMessage类，包含SwallowMessage、TopicName以及是否需要ACK等信息
      PktTextMessage pkt = TextHandler.changeTextToPacket(e.getChannel().getRemoteAddress(), jsonStr);
      //初始化json的ObjectMapper及ACK对象
      ObjectMapper mapper = new ObjectMapper();
      TextACK textAck = new TextACK();
      textAck.setOK(true);
      //如果解析json字符串失败，返回失败ACK，reason是“Wrong json string”
      if (pkt == null) {
         logger.log(Level.ERROR, "[TextHandler]:[" + e.getChannel().getRemoteAddress() + ": " + jsonStr
               + "]:[Wrong format.]");
         textAck.setOK(false);
         textAck.setReason("Wrong json string.");
         jsonStr = mapper.writeValueAsString(textAck);
         //返回ACK
         e.getChannel().write(jsonStr);
      } else {
         //调用DAO层将SwallowMessage存入DB
         try {
            messageDAO.saveMessage(pkt.getTopicName(), pkt.getMessage());
         } catch (Exception e1) {
            //记录异常，返回失败ACK，reason是“Can not save message”
            logger.log(Level.ERROR, "[TextHandler]:[Save Message Failed.]", e1.getCause());
            textAck.setOK(false);
            textAck.setReason("Can not save message.");
         }
         //如果不要ACK，立刻返回
         if (!pkt.isACK())
            return;

         textAck.setSha1(pkt.getMessage().getSha1());
         try {
            jsonStr = mapper.writeValueAsString(textAck);
         } catch (Exception e1) {
            logger.log(Level.WARN, "[TextHandler]:[Json convert failed.]", e1.getCause());
            return;
         }
         //返回ACK的json字符串
         e.getChannel().write(jsonStr);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      logger.log(Level.WARN, "Unexpected exception from downstream.", e.getCause());
      e.getChannel().close();
   }
}
