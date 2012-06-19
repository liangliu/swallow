package com.dianping.swallow.consumer.netty;



import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.consumer.ConsumerClient;

@ChannelPipelineCoverage("all")
public class MessageClientHandler extends SimpleChannelUpstreamHandler {
 
    private static final Logger logger = Logger.getLogger(
            MessageClientHandler.class.getName());
    private String timeStamp = "0";
    private String consumerId = "001";
    private String topicId = "01";
    private ConsumerClient cClient;
    public MessageClientHandler(ConsumerClient cClient){
    	this.cClient = cClient;
    }
    @Override
    public void channelConnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) {
    	//String优化问题暂不考虑哈
    	
    }
 
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        // Send back the received message to the remote peer.
    	System.out.println("获得消息：" + e.getMessage());
    	e.getChannel().write(topicId + ":" + consumerId + ":" + timeStamp);     
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