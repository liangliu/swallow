package com.dianping.swallow.consumer.netty;



import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.consumer.ConsumerMessageType;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.consumer.ConsumerClient;

//TODO delete ?
@ChannelPipelineCoverage("all")
public class MessageClientHandler extends SimpleChannelUpstreamHandler {
 
    private static final Logger logger = Logger.getLogger(
            MessageClientHandler.class.getName());
    
    private ConsumerClient cClient;
    
    private PktConsumerMessage consumermessage;
    
    public MessageClientHandler(ConsumerClient cClient){
    	this.cClient = cClient;
    }
    @Override
    public void channelConnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) {
    	
    	consumermessage = new PktConsumerMessage(ConsumerMessageType.GREET, cClient.getConsumerId(), cClient.getDest(), cClient.getConsumerType());
    	e.getChannel().write(consumermessage);   
    	
    }
 
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    	
    	SwallowMessage swallowMessage = (SwallowMessage)((PktMessage)e.getMessage()).getContent();
    	Long messageId = swallowMessage.getMessageId();    	
    	consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK ,messageId);
    	//TODO 异常处理
    	cClient.getListener().onMessage(swallowMessage);
    	e.getChannel().write(consumermessage);
    	
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