package com.dianping.swallow.producerserver.impl;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import com.dianping.swallow.common.codec.JsonDecoder;
import com.dianping.swallow.common.codec.JsonEncoder;

public class ProducerServerTextPipelineFactory implements ChannelPipelineFactory{
	private ProducerServer producerServer;
	
	public ProducerServerTextPipelineFactory(ProducerServer producerServer){
		this.producerServer = producerServer;
	}
	
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();

		pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
		
		pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
		
		pipeline.addLast("handler", new ProducerServerTextHandler(producerServer));
		
		return pipeline;
	}
}