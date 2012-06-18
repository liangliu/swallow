package com.dianping.swallow.producerserver.impl;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

public class ProducerServerTextPipelineFactory implements ChannelPipelineFactory{
	private ProducerServerText producerServerText;
	
	public ProducerServerTextPipelineFactory(ProducerServerText producerServerText){
		this.producerServerText = producerServerText;
	}
	
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		// TODO Auto-generated method stub
		ChannelPipeline pipeline = Channels.pipeline();
		
		pipeline.addLast("decoder", new StringDecoder());
		pipeline.addLast("encoder", new StringEncoder());
		pipeline.addLast("handler", new ProducerServerTextHandler(producerServerText));
		
		return pipeline;
	}
}
