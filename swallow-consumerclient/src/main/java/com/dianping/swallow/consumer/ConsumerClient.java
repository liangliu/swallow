package com.dianping.swallow.consumer;

import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import com.dianping.swallow.common.codec.JsonDecoder;
import com.dianping.swallow.common.codec.JsonEncoder;
import com.dianping.swallow.consumer.netty.MessageClientHandler;

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
            pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            pipeline.addLast("jsonDecoder", new JsonDecoder());
            pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            pipeline.addLast("jsonEncoder", new JsonEncoder());
            pipeline.addLast("handler", handler);
            return pipeline;  
            }  
        }); 
    }
    
}