package com.dianping.swallow.producerserver.impl;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

public class ProducerServerTextPipelineFactory implements ChannelPipelineFactory{
	private ProducerServer producerServer;
	
	public ProducerServerTextPipelineFactory(ProducerServer producerServer){
		this.producerServer = producerServer;
	}
	
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();
		//TODO 使用克柱的encoder,decoder
		pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
		pipeline.addLast("decoder", new StringDecoder());
		pipeline.addLast("encoder", new StringEncoder());
		pipeline.addLast("handler", new ProducerServerTextHandler(producerServer));
		
		return pipeline;
	}
}