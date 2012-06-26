package com.dianping.swallow.consumerserver.netty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
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
import com.dianping.swallow.consumerserver.HandleACKThread;
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
    	final Channel channel = e.getChannel();
    	
    	PktConsumerACK consumerACKPacket = (PktConsumerACK)e.getMessage();
    	
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