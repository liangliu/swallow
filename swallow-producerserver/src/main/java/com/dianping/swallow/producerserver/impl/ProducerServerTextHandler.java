package com.dianping.swallow.producerserver.impl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.producerserver.util.TextHandler;

public class ProducerServerTextHandler extends SimpleChannelUpstreamHandler{
	private ProducerServerText producerServerText;
	
    private static final Logger logger = Logger.getLogger(
    		ProducerServerTextHandler.class);
    
    public ProducerServerTextHandler(ProducerServerText producerServerText) {
		// TODO Auto-generated constructor stub
    	this.producerServerText = producerServerText;
	}

	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		// TODO Auto-generated method stub
    	if (e instanceof ChannelStateEvent) {
    		logger.info(e.toString());
    	}
    	super.handleUpstream(ctx, e);
	}

    @Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		// TODO Auto-generated method stub
        String request = (String) e.getMessage();
        PktStringMessage pkt = (PktStringMessage) TextHandler.changeTextToPacket(e.getChannel().getRemoteAddress(), request);
        if(pkt == null){
        	e.getChannel().write("Wrong format!");
        }else{
        	e.getChannel().write("Message is sent!");
        }
    }
    
    @Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		// TODO Auto-generated method stub
		logger.log(Level.WARN, "Unexpected exception from downstream.", e.getCause());
		e.getChannel().close();
	}
}
