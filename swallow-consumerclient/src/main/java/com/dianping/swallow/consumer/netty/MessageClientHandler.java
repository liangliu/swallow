package com.dianping.swallow.consumer.netty;




import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.consumer.ConsumerMessageType;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.consumer.ConsumerClient;

public class MessageClientHandler extends SimpleChannelUpstreamHandler {
 
    private static final Logger logger = LoggerFactory.getLogger(
            MessageClientHandler.class.getName());
    
    private ConsumerClient cClient;
    
    private PktConsumerMessage consumermessage;
    
    public MessageClientHandler(ConsumerClient cClient){
    	this.cClient = cClient;
    }
    @Override
    public void channelConnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) {
    	
    	consumermessage = new PktConsumerMessage(ConsumerMessageType.GREET, cClient.getConsumerId(), cClient.getDest(), cClient.getConsumerType(), cClient.getThreadCount());
    	e.getChannel().write(consumermessage);   
    	//如果是多线程，则除了greet消息外，仍需发送threadCount-1次ACK。
    	if(cClient.getThreadCount() > 1){
    	   int threadCount = cClient.getThreadCount();
    	   for(int i = 1; i < threadCount; i++){
    	     consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, null , cClient.getNeedClose());
           e.getChannel().write(consumermessage); 
    	   }
    	}    	
    }
 
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
       
    	cClient.getListener().onMessage(e);
    	
    }
 
    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an exception is raised.
        logger.error("exception caught, disconnect", e.getCause());
        e.getChannel().close();
    }
}