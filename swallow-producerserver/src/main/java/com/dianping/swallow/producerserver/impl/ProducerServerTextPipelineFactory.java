package com.dianping.swallow.producerserver.impl;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

public class ProducerServerTextPipelineFactory implements ChannelPipelineFactory {
   private ProducerServer producerServer;

   public ProducerServerTextPipelineFactory(ProducerServer producerServer) {
      this.producerServer = producerServer;
   }

   @Override
   public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();

      pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4));

      pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));

      pipeline.addLast("handler", new ProducerServerTextHandler(producerServer));

      return pipeline;
   }
}
