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
	int i = 0;
    private static final Logger logger = Logger.getLogger(
            MessageServerHandler.class.getName());
//    //channel的连接状态
//    public static Map<String, HashMap<Channel, String>> channelWorkStatue = new HashMap<String, HashMap<Channel, String>>();
//    //Map<Channel,Boolean> channelStatue = new HashMap<Channel,Boolean>();
//    public static ChannelGroup channelGroup = new DefaultChannelGroup();
//    //一个consumerId对应一个thread，这是对各thread的状态的管理
//    private static Map<String, Boolean> threads = new HashMap<String, Boolean>();
//    //每个consumerID对应最大TimeStamp
//    public static Map<String, BSONTimestamp> maxTimeStamp = new HashMap<String, BSONTimestamp>();
//    private MQThreadFactory threadFactory = new MQThreadFactory();
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
    	ConsumerService.cService.updateChannelWorkStatue(consumerId, channel);
    	//对应consumerID的线程不存在,应该是可以用threadslist代替。
    	if(!ConsumerService.cService.getThreads().containsKey(consumerId)){
    		ConsumerService.cService.newThread(consumerId, topicId);   		
    	}   	    	
    }
 
    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(
                Level.WARNING,
        "客户端断开连接！");
        Channel channel = e.getChannel();
        channel.close();
    }
}