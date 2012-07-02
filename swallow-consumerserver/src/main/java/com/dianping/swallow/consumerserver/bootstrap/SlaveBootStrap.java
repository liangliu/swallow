package com.dianping.swallow.consumerserver.bootstrap;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.codec.JsonDecoder;
import com.dianping.swallow.common.codec.JsonEncoder;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.consumerserver.netty.MessageServerHandler;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class SlaveBootStrap {

	//TODO 是否lion中
	private static int port = 8082;
	
	private static boolean isSlave = true;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		while(true){
			ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[]{"applicationContext.xml"});
			final ConsumerWorkerManager consumerWorkerManager = ctx.getBean(ConsumerWorkerManager.class);
			consumerWorkerManager.init(isSlave);
			// Configure the server.
	        ServerBootstrap bootstrap = new ServerBootstrap(
	                new NioServerSocketChannelFactory(
	                        Executors.newCachedThreadPool(),
	                        Executors.newCachedThreadPool()));
	        
	        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {  
	            @Override  
	            public ChannelPipeline getPipeline() throws Exception {  
	            MessageServerHandler handler = new MessageServerHandler(consumerWorkerManager);
	            ChannelPipeline pipeline = Channels.pipeline();
	            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
	            pipeline.addLast("jsonDecoder", new JsonDecoder(PktConsumerMessage.class));
	            pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
	            pipeline.addLast("jsonEncoder", new JsonEncoder(PktMessage.class));
	            pipeline.addLast("handler", handler);
	            return pipeline;  
	            }  
	        });  
	        // Bind and start to accept incoming connections.
	       bootstrap.bind(new InetSocketAddress(port));
	       consumerWorkerManager.checkMasterIsLive(bootstrap);
		} 
	}

}
