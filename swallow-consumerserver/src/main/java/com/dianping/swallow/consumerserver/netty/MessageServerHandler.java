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

import com.dianping.swallow.consumerserver.ConsumerService;



@ChannelPipelineCoverage("all")
public class MessageServerHandler extends SimpleChannelUpstreamHandler {
	private ConsumerService cService;
	public MessageServerHandler(ConsumerService cService){
		this.cService = cService;
	}
	//TODO log4j
    private static final Logger logger = Logger.getLogger(
            MessageServerHandler.class.getName());
    @Override  
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)  
        throws Exception {     
    	e.getChannel().write("come on baby");
    }
 
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    	Channel channel = e.getChannel();
    	String message = (String)e.getMessage();
    	String topicId = message.split(":")[0];
    	String consumerId = message.split(":")[1];
    	String timeStamp = message.split(":")[2];
    	cService.putChannelToBlockQueue(consumerId, channel);
    	//对应consumerID的线程不存在,应该是可以用threadslist代替。
    	//线程安全
    	cService.updateThreadWorkStatues(consumerId, topicId);
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