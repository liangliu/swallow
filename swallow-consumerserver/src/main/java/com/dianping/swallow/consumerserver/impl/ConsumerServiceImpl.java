package com.dianping.swallow.consumerserver.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.Channel;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.consumerserver.ConsumerService;
import com.dianping.swallow.consumerserver.GetMessageThread;
import com.dianping.swallow.consumerserver.Heartbeater;
import com.dianping.swallow.consumerserver.MQThreadFactory;
import com.dianping.swallow.consumerserver.config.ConfigManager;
import com.dianping.swallow.consumerserver.util.MongoUtil;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class ConsumerServiceImpl implements ConsumerService{
	
	private ConfigManager configManager;
	
	private Mongo mongo;

	private Map<String, HashSet<Channel>> channelWorkStatue;	//channel是否存在的状态
    
    private Map<String, Semaphore> freeChannelNums = new HashMap<String, Semaphore>();
    
    private Map<String, String> preparedMesssages = new HashMap<String, String>();
    
    private String message = null;
    
    //一个consumerId对应一个thread，这是对各thread的状态的管理
    private Map<String, Boolean> threads = new HashMap<String, Boolean>();
    
    private MQThreadFactory threadFactory;
    
    private Map<String, ArrayBlockingQueue<String>> messageQueue = new HashMap<String, ArrayBlockingQueue<String>>();
    
    private Map<String, ArrayBlockingQueue<Channel>> freeChannelQueue = new HashMap<String, ArrayBlockingQueue<Channel>>();
    

    
    private ArrayBlockingQueue<Channel> freeChannels;
    
    @Autowired
    private Heartbeater heartbeater;
	   
    public Map<String, ArrayBlockingQueue<String>> getMessageQueue() {
		return messageQueue;
	}
    
	public Map<String, ArrayBlockingQueue<Channel>> getFreeChannelQueue() {
		return freeChannelQueue;
	}

	public Mongo getMongo() {
		return mongo;
	}

	public ConfigManager getConfigManager() {
		return configManager;
	}
	
	
	public Map<String, HashSet<Channel>> getChannelWorkStatue() {
		return channelWorkStatue;
	}

	public Map<String, Boolean> getThreads() {
		return threads;
	}
	
	public ConsumerServiceImpl(String uri){    	
    	this.channelWorkStatue = new HashMap<String, HashSet<Channel>>();
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
	public void changeChannelWorkStatue(String consumerId, Channel channel){
		synchronized(channelWorkStatue){
			if(channelWorkStatue.get(consumerId) == null){
				HashSet<Channel> channels = new HashSet<Channel>();
				channels.add(channel);
				channelWorkStatue.put(consumerId, channels);
			} else{
				HashSet<Channel> channels = channelWorkStatue.get(consumerId);
				channels.add(channel);
			}
		}    
	}
	
	public void putChannelToBlockQueue(String consumerId, Channel channel){			
		freeChannels = freeChannelQueue.get(consumerId);
		if(freeChannels == null){
			freeChannels = new ArrayBlockingQueue<Channel>(configManager.getFreeChannelBlockQueueSize());
			freeChannelQueue.put(consumerId, freeChannels);
		}
		try {
			freeChannels.put(channel);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
		
	public void updateThreadWorkStatues(String consumerId, String topicId){
		synchronized(threads){
			if(!threads.containsKey(consumerId)){
				GetMessageThread server = new GetMessageThread();
				server.setConsumerId(consumerId);
				server.setTopicId(topicId);
				server.setcService(this);
				Thread t = threadFactory.newThread(server, topicId + consumerId + "-consumer-");
		    	t.start();
		    	threads.put(consumerId, Boolean.TRUE);
			}    
		}			
    }
	
	public void ergodicChannelByCId(String consumerId,String topicId){
		
		freeChannels = freeChannelQueue.get(consumerId);
		ArrayBlockingQueue<String> messages = messageQueue.get(consumerId);
		try {
			while(true){
				Channel channel = freeChannels.poll(configManager.getFreeChannelBlockQueueOutTime(),TimeUnit.MILLISECONDS);
				if(channel == null){
					break;
				}else if(channel.isConnected()){
					if(preparedMesssages.get(consumerId) != null){
						message = preparedMesssages.get(consumerId);
						preparedMesssages.remove(consumerId);
					} else{					
						//String message = messages.take();
						message = "假的";
					}
					//TODO 从消息中得到maxTStamp
					//TODO 是否可以自己设置BSongtimestamp
					BSONTimestamp maxTStamp = new BSONTimestamp();
					//更新mongo中最大timeStamp
					//TODO 受到ack再更新
//					dao.addCounter(topicId, consumerId,maxTStamp);
					if(!channel.isConnected()){
						preparedMesssages.put(consumerId, message);
					} else{
						channel.write(message);
					}
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//获得同consumerId下所有的channel
//		HashMap<Channel, String> channels = channelWorkStatue.get(consumerId);
//		Iterator<Entry<Channel, String>> iterator = channels.entrySet().iterator();
//		ArrayBlockingQueue<String> messages = messageQueue.get(consumerId);
//		while(iterator.hasNext()){
//			Entry<Channel, String> entry = iterator.next();
//			try {
//				//TODO 
//				boolean isTimeOut = freeChannelNums.get(consumerId).tryAcquire(configManager.getSemaphoreTimeOutTime(),TimeUnit.MILLISECONDS);
//				if(!isTimeOut){
//					break;
//				}
//				freeChannelNums.get(consumerId).release();
//			} catch (InterruptedException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			if("done".equals(entry.getValue())){
//				//TODO 换成message的类，暂时用String代替吧。
//				if(preparedMesssages.get(consumerId) != null){
//					message = preparedMesssages.get(consumerId);
//					preparedMesssages.remove(consumerId);
//				} else{
//					//用blockingqueue就不用在内存中记录最大timeStamp了。
//					try {
//						String message = messages.take();
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					//TODO 从消息中得到maxTStamp
//					//TODO 是否可以自己设置BSongtimestamp
//					BSONTimestamp maxTStamp = new BSONTimestamp();
//					//更新mongo中最大timeStamp
//					//TODO 受到ack再更新
//					dao.addCounter(topicId, consumerId,maxTStamp);
//				}										
//				Channel channel = entry.getKey();
//				//如果此时连接断了，则把消息存到预发消息变量中。
//				if(!channel.isConnected()){
//					preparedMesssages.put(consumerId, message);
//				} else{
//					//TODO 如何防止内存使用过量，问游泳
//					channel.write(message);
//					channels.put(channel, "doing");
//					try {
//						freeChannelNums.get(consumerId).acquire();
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//			}
//		}		
	}
	
	public void changeStatuesWhenChannelBreak(Channel channel){
		
//		String cId = null;
		HashSet<Channel> channels = null;
		synchronized(channelWorkStatue){
			Iterator<Map.Entry<String,HashSet<Channel>>> iterator = channelWorkStatue.entrySet().iterator();
			while(iterator.hasNext()){
				Entry<String, HashSet<Channel>> entry = iterator.next();
				channels = entry.getValue();
				if(channels.contains(channel)){	
					channels.remove(channel);
					break;
				}
			}
		}
		
//		channels = channelWorkStatue.get(cId);
//		if("done".equals(channels.get(channel))){
//			try {
//				freeChannelNums.get(cId).acquire();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		channels.remove(cId);
//		synchronized(threads){
//			if(channels.isEmpty()){
//				threads.remove(cId);						
//			}
//		}
		
	}
	
	public void init(boolean isSlave){
		if (isSlave) {
			try {
				// wont throw MongoException
				heartbeater.waitUntilStopBeating(configManager.getMasterIp(),configManager.getHeartbeatCheckInterval(),configManager.getHeartbeatMaxStopTime());
			} catch (InterruptedException e) {
				return;
			}
		} else{
			startHeartbeater(configManager.getMasterIp());
		}
		
	}
	private void startHeartbeater(final String ip) {
		
		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				while (true) {
					
					try {
						heartbeater.beat(ip);
						Thread.sleep(configManager.getHeartbeatUpdateInterval());
					} catch (Exception e) {
						//log.error("Error update heart beat", e);
					}
				}
			}

		};

		Thread heartbeatThread = threadFactory.newThread(runnable, "heartbeat-");
		heartbeatThread.setDaemon(true);
		heartbeatThread.start();
	}
}
