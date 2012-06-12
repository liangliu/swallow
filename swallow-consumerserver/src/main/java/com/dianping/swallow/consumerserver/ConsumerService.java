package com.dianping.swallow.consumerserver;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.Channel;

import com.dianping.swallow.consumerserver.config.ConfigManager;


public class ConsumerService {

	private ConfigManager configManager;
	//channel的连接状态
    private Map<String, HashMap<Channel, String>> channelWorkStatue;
    
    //一个consumerId对应一个thread，这是对各thread的状态的管理
    private Map<String, Boolean> threads = new HashMap<String, Boolean>();
    
    private MQThreadFactory threadFactory;
    
    private Map<String, ArrayBlockingQueue<String>> messageQueue = new HashMap<String, ArrayBlockingQueue<String>>();
	   
    public Map<String, ArrayBlockingQueue<String>> getMessageQueue() {
		return messageQueue;
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
	public ConsumerService(){    	
    	this.channelWorkStatue = new HashMap<String, HashMap<Channel, String>>();
    	this.configManager = ConfigManager.getInstance();
    	this.threadFactory = new MQThreadFactory();
    }
    //有新消息到的时候，更新channel的状态
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
}
