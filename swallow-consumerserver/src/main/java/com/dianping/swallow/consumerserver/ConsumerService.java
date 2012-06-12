package com.dianping.swallow.consumerserver;


import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.dianping.swallow.consumerserver.config.ConfigManager;
import com.dianping.swallow.consumerserver.netty.MessageServerPipelineFactory;
import com.dianping.swallow.consumerserver.util.MongoUtil;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;


public class ConsumerService {
	//TODO 是否lion中
	private static int port = 8081;
	private ConfigManager configManager;
	public static ConsumerService cService;
	private Mongo mongo;
	//channel的连接状态
    private Map<String, HashMap<Channel, String>> channelWorkStatue;
    
    //一个consumerId对应一个thread，这是对各thread的状态的管理
    private Map<String, Boolean> threads = new HashMap<String, Boolean>();
    
    private MQThreadFactory threadFactory;
    
    private Map<String, ArrayBlockingQueue<String>> messageQueue = new HashMap<String, ArrayBlockingQueue<String>>();
	   
    public Map<String, ArrayBlockingQueue<String>> getMessageQueue() {
		return messageQueue;
	}
    
	public Mongo getMongo() {
		return mongo;
	}

	public ConfigManager getConfigManager() {
		return configManager;
	}
	
	public Map<String, HashMap<Channel, String>> getChannelWorkStatue() {
		return channelWorkStatue;
	}
	
	public Map<String, Boolean> getThreads() {
		return threads;
	}
	
	public ConsumerService(String uri){    	
    	this.channelWorkStatue = new HashMap<String, HashMap<Channel, String>>();
    	this.configManager = ConfigManager.getInstance();
    	this.threadFactory = new MQThreadFactory();
    	List<ServerAddress> replicaSetSeeds = MongoUtil.parseUri(uri);
		mongo = new Mongo(replicaSetSeeds, getDefaultOptions());
    }
	
	//一些option暂时设置好，先不提供
	private MongoOptions getDefaultOptions() {
		MongoOptions options = new MongoOptions();
//		options.slaveOk = config.isMongoSlaveOk();
//		options.socketKeepAlive = config.isMongoSocketKeepAlive();
//		options.socketTimeout = config.getMongoSocketTimeout();
//		options.connectionsPerHost = config.getMongoConnectionsPerHost();
//		options.threadsAllowedToBlockForConnectionMultiplier = config
//				.getMongoThreadsAllowedToBlockForConnectionMultiplier();
//		options.w = config.getMongoW();
//		options.wtimeout = config.getMongoWtimeout();
//		options.fsync = config.isMongoFsync();
//		options.connectTimeout = config.getMongoConnectTimeout();
//		options.maxWaitTime = config.getMongoMaxWaitTime();
//		options.autoConnectRetry = config.isMongoAutoConnectRetry();
//		options.safe = config.isMongoSafe();
		return options;
	}
	
    //有新消息到的时候，更新channel的状态
	//TODO 线程安全，方法名
	public void updateChannelWorkStatue(String consumerId, Channel channel){
    	if(channelWorkStatue.get(consumerId) == null){
			HashMap<Channel, String> channels = new HashMap<Channel, String>();
			channels.put(channel, "done");
			channelWorkStatue.put(consumerId, channels);
		} else{
			HashMap<Channel, String> channels = channelWorkStatue.get(consumerId);
			channels.put(channel, "done");
		}
    }
    
    //对应没有线程的consumerId,创建新线程
	public void newThread(String consumerId, String topicId){
    	GetMessageThread server = new GetMessageThread();
		server.setConsumerId(consumerId);
		server.setTopicId(topicId);
		Thread t = threadFactory.newThread(server, topicId + consumerId + "-consumer-");
    	t.start();
    	threads.put(consumerId, Boolean.TRUE);
    }
	
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
    }
}
