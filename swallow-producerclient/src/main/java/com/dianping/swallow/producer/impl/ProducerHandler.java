/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-5
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.producer.impl;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * TODO Comment of ProducerHandlers
 * @author tong.song
 *
 */
public class ProducerHandler extends SimpleChannelUpstreamHandler{
	Producer producer;
	
	public ProducerHandler(Producer producer){
		this.producer = producer;
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		// TODO Auto-generated method stub
		super.channelConnected(ctx, e);
		
		PkgProducerGreet producerGreet = new PkgProducerGreet("You are a pig");
		e.getChannel().write(producerGreet);
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		// TODO Auto-generated method stub
		super.messageReceived(ctx, e);
		
		Package pkg = (Package)e.getMessage();
		switch(pkg.getPackageType()){
		case SWALLOW_P_ACK:
			if(e.getRemoteAddress().equals(producer.getCurrentSwallowP()) && ((PkgSwallowPACK)pkg).getSEQ() == producer.getAckNum()){
				System.out.println("recieve ack:" + ((PkgSwallowPACK)pkg).getSEQ() + 
						" from: " + e.getRemoteAddress());
				synchronized (producer) {
					producer.notifyAll();
				}
			}else{
				System.out.println("false ack, drop it.");
			}
			break;
		default:
			break;
		}
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		// TODO Auto-generated method stub
		super.channelDisconnected(ctx, e);
		System.out.println("channel is disconnected");
	}
}
