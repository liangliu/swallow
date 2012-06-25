package com.dianping.swallow.consumerserver.netty;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.packet.PktConsumerACK;
import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;



@ChannelPipelineCoverage("all")
public class MessageServerHandler extends SimpleChannelUpstreamHandler {
	private ConsumerServiceImpl cService;
	public MessageServerHandler(ConsumerServiceImpl cService){
		this.cService = cService;
	}
	//TODO log4j
    private static final Logger logger = Logger.getLogger(
            MessageServerHandler.class.getName());
    @Override  
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)  
        throws Exception {     
    	//e.getChannel().write("come on baby");
    }
 
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    	//收到PktConsumerACK，按照原流程解析
    	Channel channel = e.getChannel();
    	PktConsumerACK consumerACKPacket = (PktConsumerACK)e.getMessage();
    	String topicName = consumerACKPacket.getDest().getName();
    	String consumerId = consumerACKPacket.getConsumerId();
    	ConsumerType consumerType = consumerACKPacket.getConsumerType();
    	//TODO 记录日志
    	Long messageId = consumerACKPacket.getMessageId();
    	if(cService.getConsumerTypes().get(consumerId) == null){
    		cService.getConsumerTypes().put(consumerId, consumerType);
    	}
    	if(messageId != null && ConsumerType.UPDATE_AFTER_ACK.equals(cService.getConsumerTypes().get(consumerId))){
    		cService.updateMaxMessageId(topicName, consumerId, messageId);
    	}
    	cService.putChannelToBlockQueue(consumerId, channel);
    	cService.addThread(consumerId, topicName);
    }
 
    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(
                Level.WARNING,
        "客户端断开连接！");
        Channel channel = e.getChannel();
        cService.changeStatuesWhenChannelBreak(channel);
        channel.close();
    }
}