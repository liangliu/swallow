package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;


public class MessageClientC {
	//应该都是取自于lion
	public static String host = "127.0.0.1";
//	public static String host = "10.1.14.79";
	public static int marsterPort = 8081;
	public static int slavePort = 8082;
    public static void main(String[] args) throws Exception {
    	
    	ConsumerClient cClient = new ConsumerClient();  	
	   	cClient.init();
	   	ClientBootstrap bootstrap = cClient.getBootstrap();
	   	ConSlaveThread slave = new ConSlaveThread();
    	slave.setBootstrap(bootstrap);
	   	Thread slaveThread = new Thread(slave);
	   	slaveThread.start();
	   	while(true){
	   		synchronized(bootstrap){
		   		ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, marsterPort));
		   		future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞!	
		   	}
	   		Thread.sleep(1000);//TODO 配置变量
	   	}	   		   		   	
    	
    }
}