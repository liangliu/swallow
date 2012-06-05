/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-24
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
package com.dianping.swallow.producer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import com.dianping.swallow.MQService;

/**
 * TODO Comment of SwallowP
 * @author tong.song
 *
 */
public class SwallowP {
//	private String		MONGO_URI	=	"192.168.31.178:27016";
//	private	MQService	sqs			=	new MongoMQService(MONGO_URI);
	private String		host;
	private int			port;
	
	public SwallowP(String host, int port){
		this.host = host;
		this.port = port;
	}
	
	public void run(){
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(), 
						Executors.newCachedThreadPool()
						)
				);
		bootstrap.setPipelineFactory(
				new ChannelPipelineFactory() {					
					@Override
					public ChannelPipeline getPipeline() throws Exception {
						// TODO Auto-generated method stub
						return Channels.pipeline(
								new ObjectEncoder(),
								new ObjectDecoder(),
								new SwallowPHandler()
								);
					}
				}
				);
		bootstrap.bind(new InetSocketAddress(port));
		System.out.println("Server is ready");
	}
	
	private class SwallowPHandler extends SimpleChannelUpstreamHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			// TODO Auto-generated method stub
			super.messageReceived(ctx, e);
			
			Packet packet = (Packet) e.getMessage();
			switch(packet.getPacketHead()){
			case PRODUCER_GREET:
				String version = ((PktProducerGreet) packet).getProducerVersion();
				System.out.println("Producer version: " + version);
				e.getChannel().write(new PktSwallowPACK(port));
				break;
			case STRING_MSG:
				PktStringMessage stringMsg = (PktStringMessage)packet;
				System.out.println(stringMsg.getContent());
				e.getChannel().write(new PktSwallowPACK(stringMsg.getAckNum()));
				System.out.println("ack sent");
				break;
			case BINARY_MSG:
				break;
			default:
				break;
			}
		}
	}

	public static void main(String[] args){
		int port = 8081;
		new SwallowP("127.0.0.1", port).run();
	}
}