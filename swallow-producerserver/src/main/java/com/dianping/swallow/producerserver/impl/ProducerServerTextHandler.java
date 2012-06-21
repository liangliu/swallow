package com.dianping.swallow.producerserver.impl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.common.packet.PktObjectMessage;
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
        	logger.info("TextMessage { " + e.getChannel().getRemoteAddress() + ": " + request + "} [Wrong format.]");
        	e.getChannel().write("Wrong format.");
        }else{
        	PktObjectMessage	objMsg = pkt.getObjMsg();
        	PktSwallowPACK		ack = (PktSwallowPACK)producerServer.sendMessage(objMsg);
        	if(pkt.isACK()){
        		//保存成功则返回消息的SHA-1字符串
        		//TODO 返回是否成功等信息，json?
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
