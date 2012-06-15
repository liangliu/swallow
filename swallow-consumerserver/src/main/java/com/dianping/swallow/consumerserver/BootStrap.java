package com.dianping.swallow.consumerserver;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;
import com.dianping.swallow.consumerserver.netty.MessageDecoder;
import com.dianping.swallow.consumerserver.netty.MessageEncoder;
import com.dianping.swallow.consumerserver.netty.MessageServerHandler;

public class BootStrap {

	//TODO 是否lion中
	private static int port = 8081;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[]{"applicationContext.xml", "swallow.xml"});
		final ConsumerServiceImpl cService = ctx.getBean(ConsumerServiceImpl.class);
		// Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        final MessageServerHandler handler = new MessageServerHandler(cService);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {  
            @Override  
            public ChannelPipeline getPipeline() throws Exception {  
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("encode", new MessageEncoder());
            pipeline.addLast("decode", new MessageDecoder());
            pipeline.addLast("handler", handler);
            return pipeline;  
            }  
        });  
        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
	}

}
