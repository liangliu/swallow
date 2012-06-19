package com.dianping.swallow.producerserver.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class ProducerServerText {
	private ProducerServer producerServer;
	public ProducerServerText(ProducerServer producerServer){
		this.producerServer = producerServer;
	}
	public void start(){
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
		bootstrap.setPipelineFactory(new ProducerServerTextPipelineFactory(producerServer));
		bootstrap.bind(new InetSocketAddress("127.0.0.1", 5000));
	}
}
