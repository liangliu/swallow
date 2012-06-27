package com.dianping.swallow.consumer.netty;



import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.bson.types.BSONTimestamp;

import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktConsumerGreet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.consumer.ConsumerClient;

//TODO delete ?
@ChannelPipelineCoverage("all")
public class MessageClientHandler extends SimpleChannelUpstreamHandler {
 
    private static final Logger logger = Logger.getLogger(
            MessageClientHandler.class.getName());
    
    private ConsumerClient cClient;
    
    private PktConsumerGreet consumerACKPacket;
    
    public MessageClientHandler(ConsumerClient cClient){
    	this.cClient = cClient;
    }
    @Override
    public void channelConnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) {
    	
    	consumerACKPacket = new PktConsumerGreet(cClient.getConsumerId(), cClient.getDest(), cClient.getConsumerType(), null);
    	e.getChannel().write(consumerACKPacket);   
    	
    }
 
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    	
    	SwallowMessage swallowMessage = (SwallowMessage)((PktObjectMessage)e.getMessage()).getContent();
    	Long messageId = swallowMessage.getMessageId();
    	consumerACKPacket.setMessageId(messageId);
    	//TODO 异常处理
    	cClient.getListener().onMessage(swallowMessage);
    	e.getChannel().write(consumerACKPacket);
    	
    }
 
    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an exception is raised.
        logger.log(
                Level.WARNING,
                "连不上了！"
                );
        e.getChannel().close();
    }
}