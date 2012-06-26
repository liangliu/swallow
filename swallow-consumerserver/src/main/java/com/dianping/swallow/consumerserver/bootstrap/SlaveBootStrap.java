package com.dianping.swallow.consumerserver.bootstrap;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.codec.JsonDecoder;
import com.dianping.swallow.common.codec.JsonEncoder;
import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;
import com.dianping.swallow.consumerserver.netty.MessageServerHandler;

public class SlaveBootStrap {

	//TODO 是否lion中
	private static int port = 8082;
	
	private static boolean isSlave = true;
	public static Boolean slaveShouldLive = Boolean.TRUE;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		while(true){
			ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[]{"applicationContext.xml"});
			final ConsumerServiceImpl cService = ctx.getBean(ConsumerServiceImpl.class);
			cService.init(isSlave);
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
	            pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
	            pipeline.addLast("jsonDecoder", new JsonDecoder());
	            pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
	            pipeline.addLast("jsonEncoder", new JsonEncoder());
	            pipeline.addLast("handler", handler);
	            return pipeline;  
	            }  
	        });  
	        // Bind and start to accept incoming connections.
	       bootstrap.bind(new InetSocketAddress(port));
	       cService.checkMasterIsLive(bootstrap);
	       while(slaveShouldLive){
	    	   try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	       }
		} 
	}

}
