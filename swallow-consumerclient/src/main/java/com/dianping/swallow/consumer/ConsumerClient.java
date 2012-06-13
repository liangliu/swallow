package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class ConsumerClient {

	
	//连接swollowC，获得ChannelFuture
    public ChannelFuture getChannelFuture(InetSocketAddress swollowCAddr){
    	ClientBootstrap bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new MessageClientPipelineFactoryC());
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(swollowCAddr);
        return future;
    }
}