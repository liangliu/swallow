package com.dianping.swallow.producerserver.impl;

import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.producer.ProducerUtils;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServerTextHandler extends SimpleChannelUpstreamHandler {
   private final MessageDAO    messageDAO;

   //TextHandler状态代码
   private static final int    OK                = 250;
   private static final int    INVALID_TOPIC_NAME = 251;
   private static final int    SAVE_FAILED       = 252;

   private static final Logger logger            = Logger.getLogger(ProducerServerTextHandler.class);

   /**
    * 构造函数
    * 
    * @param messageDAO
    */
   public ProducerServerTextHandler(MessageDAO messageDAO) {
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
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e){
      //获取TextObject
      TextObject textObject = (TextObject) e.getMessage();
      //生成SwallowMessage//TODO 增加swallowMessage的IP信息
      SwallowMessage swallowMessage = new SwallowMessage();
      swallowMessage.setContent(textObject.getContent());
      swallowMessage.setGeneratedTime(new Date());
      swallowMessage.setSha1(SHAGenerater.generateSHA(swallowMessage.getContent()));
      swallowMessage.setSourceIp(e.getChannel().getRemoteAddress().toString());
      //初始化ACK对象
      TextACK textAck = new TextACK();
      textAck.setStatus(OK);
      //TopicName非法，返回失败ACK，reason是"TopicName is not valid."
      if (!ProducerUtils.isTopicNameValid(textObject.getTopic())) {
         logger.log(Level.ERROR, "[TextHandler]:[" + e.getChannel().getRemoteAddress() + ": " + textObject
               + "]:[Wrong format.]");
         textAck.setStatus(INVALID_TOPIC_NAME);
         textAck.setInfo("TopicName is invalid.");
         //返回ACK
         e.getChannel().write(textAck);
      } else {
         //调用DAO层将SwallowMessage存入DB
         try {
            messageDAO.saveMessage(textObject.getTopic(), swallowMessage);
         } catch (Exception e1) {
            //记录异常，返回失败ACK，reason是“Can not save message”
            logger.log(Level.ERROR, "[TextHandler]:[Save Message Failed.]", e1.getCause());
            textAck.setStatus(SAVE_FAILED);
            textAck.setInfo("Can not save message.");
         }
         //如果不要ACK，立刻返回
         if (!textObject.isACK())
            return;

         textAck.setInfo(swallowMessage.getSha1());
         //返回ACK
         e.getChannel().write(textAck);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      logger.log(Level.WARN, "Unexpected exception from downstream.", e.getCause());
      e.getChannel().close();
   }
}
