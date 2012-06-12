package com.dianping.swallow.consumerserver.netty;

import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;



public class MessageServerPipelineFactory implements
        ChannelPipelineFactory {

	public ChannelPipeline getPipeline() throws Exception {
    	
    	 ChannelFactory factory =  
             new NioServerSocketChannelFactory (  
                     Executors.newCachedThreadPool(),  
                     Executors.newCachedThreadPool());  
    	ServerBootstrap bootstrap = new ServerBootstrap (factory);  
    	  
        ChannelPipeline pipeline = bootstrap.getPipeline();  
 
        pipeline.addLast("decoder", new MessageDecoder());
        pipeline.addLast("encoder", new MessageEncoder());
        pipeline.addLast("handler", new MessageServerHandler());
 
        return pipeline;

        
    }
}