package com.dianping.swallow.consumerserver.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.dianping.swallow.consumerserver.ConsumerService;




/**
 * zhang.yu
 */

public class MessageServer {
	//TODO 是否lion中
	private static int port = 8081;
	public static ConsumerService cService;
    public static void main(String[] args) throws Exception {
    	//TODO 获取lion中的配置，主要是mongo的地址！    	
    	String uri = "192.168.31.178:27016";
    	cService = new ConsumerService(uri);
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        //读取配置文件，把线程启起来
        bootstrap.setPipelineFactory(new MessageServerPipelineFactory());
        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
       // Test a = new Test();
        //a.test();
       // ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
//        CounterDAO s = (CounterDAO) ctx.getBean("dao");
//        s.getMaxTimeStamp("1", "1");
    }
}