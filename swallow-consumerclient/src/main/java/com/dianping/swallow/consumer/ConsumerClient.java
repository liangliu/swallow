package com.dianping.swallow.consumer;

import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.dianping.swallow.consumer.netty.MessageClientHandler;
import com.dianping.swallow.consumer.netty.MessageDecoder;
import com.dianping.swallow.consumer.netty.MessageEncoder;

public class ConsumerClient {

	private ClientBootstrap bootstrap;
	
	public ClientBootstrap getBootstrap() {
		return bootstrap;
	}

	//连接swollowC，获得ChannelFuture
    public void init(){
    	bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        final MessageClientHandler handler = new MessageClientHandler(this);
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
    }
    
}