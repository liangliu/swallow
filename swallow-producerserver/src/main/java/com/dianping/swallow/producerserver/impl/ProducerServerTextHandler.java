package com.dianping.swallow.producerserver.impl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.producerserver.util.TextHandler;

public class ProducerServerTextHandler extends SimpleChannelUpstreamHandler{
	private ProducerServer producerServer;
	
    private static final Logger logger = Logger.getLogger(
    		ProducerServerTextHandler.class);
    
    public ProducerServerTextHandler(ProducerServer producerServer) {
    	this.producerServer = producerServer;
	}

	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
    	if (e instanceof ChannelStateEvent) {
    		logger.info(e.toString());
    	}
    	super.handleUpstream(ctx, e);
	}

    @Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
        String request = (String) e.getMessage();
        PktTextMessage pkt = TextHandler.changeTextToPacket(e.getChannel().getRemoteAddress(), request);
        if(pkt == null){
        	e.getChannel().write("Wrong format!\r\n");
        	//TODO: log
        }else{
        	PktSwallowPACK ack = (PktSwallowPACK)producerServer.sendMessage(pkt);
        	if(pkt.isACK()){
        		e.getChannel().write(ack.getShaInfo());
        	}
        }
    }
    
    @Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.log(Level.WARN, "Unexpected exception from downstream.", e.getCause());
		e.getChannel().close();
	}
}
