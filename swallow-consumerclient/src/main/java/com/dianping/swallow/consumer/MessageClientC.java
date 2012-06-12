package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;


public class MessageClientC {
	//应该都是取自于lion
	static String host = "127.0.0.1";
	static int marsterPort = 8081;
	static int slavePort = 8082;
    public static void main(String[] args) throws Exception {        
        ClientBootstrap bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new MessageClientPipelineFactoryC());
        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, marsterPort));
        // Wait until the connection is closed or the connection attempt fails.
        future.getChannel().getCloseFuture().awaitUninterruptibly();
        // Shut down thread pools to exit.
        bootstrap.releaseExternalResources();
    }
}