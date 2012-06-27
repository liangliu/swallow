package com.dianping.swallow.consumerserver.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.dao.impl.mongodb.MongoUtils;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.consumerserver.ChannelInformation;
import com.dianping.swallow.consumerserver.ConsumerService;
import com.dianping.swallow.consumerserver.GetMessageThread;
import com.dianping.swallow.consumerserver.HandleACKThread;
import com.dianping.swallow.consumerserver.Heartbeater;
import com.dianping.swallow.consumerserver.MQThreadFactory;
import com.dianping.swallow.consumerserver.buffer.SwallowBuffer;
import com.dianping.swallow.consumerserver.config.ConfigManager;
import com.dianping.swallow.consumerserver.util.MongoUtil;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class ConsumerServiceImpl implements ConsumerService{
	
	private ConfigManager configManager;
	
	private Mongo mongo;

	private Map<String, HashSet<Channel>> channelWorkStatus;	//channel是否存在的状态
    
    private Map<String, PktObjectMessage> preparedMesssages = new HashMap<String, PktObjectMessage>();
    
    private SwallowMessage message = null;
    
    private PktObjectMessage pktMsg = null;
    
    @Autowired
    private SwallowBuffer swallowBuffer;
    
    private  Map<Channel, ChannelInformation> channelInformations = new HashMap<Channel, ChannelInformation>();
    
    //一个consumerId对应一个thread，这是对各thread的状态的管理
    private Set<String> threads = new HashSet<String>();
    
    private MQThreadFactory threadFactory;
   
    private Map<String, ConsumerType> consumerTypes = new HashMap<String, ConsumerType>();
    
    private Set<String> getMessageThreadStatus = new HashSet<String>();
    
    private Map<String, ArrayBlockingQueue<Channel>> freeChannels = new HashMap<String, ArrayBlockingQueue<Channel>>();
    
    @Autowired
    private AckDAO ackDao;
    
    @Autowired
    private MessageDAO messageDao;
       
    private ArrayBlockingQueue<Channel> freeChannelQueue;
    
    @Autowired
    private Heartbeater heartbeater;
    
    private Map<String, ArrayBlockingQueue<Runnable>> getAckWorkers = new HashMap<String, ArrayBlockingQueue<Runnable>>();
	
	public Map<String, ArrayBlockingQueue<Runnable>> getGetAckWorkers() {
		return getAckWorkers;
	}
    
	public Map<String, ArrayBlockingQueue<Channel>> getFreeChannels() {
		return freeChannels;
	}

	public Set<String> getGetMessageThreadStatus() {
		return getMessageThreadStatus;
	}

	public Map<String, ConsumerType> getConsumerTypes() {
		return consumerTypes;
	}

	public Mongo getMongo() {
		return mongo;
	}

	public ConfigManager getConfigManager() {
		return configManager;
	}
	
	
	public Map<String, HashSet<Channel>> getChannelWorkStatus() {
		return channelWorkStatus;
	}

	public Set<String> getThreads() {
		return threads;
	}
	
	public ConsumerServiceImpl(String uri){    	
    	this.channelWorkStatus = new HashMap<String, HashSet<Channel>>();
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
	public void changeChannelWorkStatus(String consumerId, Channel channel){
		synchronized(channelWorkStatus){
			if(channelWorkStatus.get(consumerId) == null){
				HashSet<Channel> channels = new HashSet<Channel>();
				channels.add(channel);
				channelWorkStatus.put(consumerId, channels);
			} else{
				HashSet<Channel> channels = channelWorkStatus.get(consumerId);
				channels.add(channel);
			}
		}    
	}
	
	public void putChannelToBlockQueue(String consumerId, Channel channel){
		//freeChannels应该是容量无上限的
		freeChannelQueue = freeChannels.get(consumerId);
		synchronized(freeChannelQueue){
			if(freeChannelQueue == null){
				freeChannelQueue = new ArrayBlockingQueue<Channel>(configManager.getFreeChannelBlockQueueSize());
				freeChannels.put(consumerId, freeChannelQueue);
			}
			try {
				freeChannelQueue.put(channel);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
    }
	
	private void updateMaxMessageId(String consumerId, String topicName, Long messageId){	
		if(messageId != null && ConsumerType.UPDATE_AFTER_ACK.equals(consumerTypes.get(consumerId))){
			ackDao.add(topicName, consumerId, messageId);
		}
    }
	
	public void addHandleACKThread(String consumerId, String topicName, ArrayBlockingQueue<Runnable> getAckWorker){
				
		HandleACKThread handleACKThread = new HandleACKThread();
    	handleACKThread.setGetAckWorker(getAckWorker);
    	handleACKThread.setConsumerId(consumerId);
    	handleACKThread.setcService(this);
    	Thread thread2 = threadFactory.newThread(handleACKThread, topicName + consumerId + "-handleACKThread-");
    	thread2.start();
    }
	
	public void addGetMessageThread(String consumerId, String topicName){
		
		GetMessageThread getMessageThread = new GetMessageThread();
		getMessageThread.setConsumerId(consumerId);
		getMessageThread.setTopicName(topicName);
		getMessageThread.setcService(this);
		Thread thread1 = threadFactory.newThread(getMessageThread, topicName + consumerId + "-getMessageThread-");
		thread1.start();
		
	}
	public void pollFreeChannelsByCId(String consumerId,String topicName){
		
		BlockingQueue<Message> messages = null;
		//线程刚起，第一次调用的时候，需要先去mongo中获取maxMessageId
		if(messages == null){
			long messageIdOfTailMessage = getMessageIdOfTailMessage(topicName, consumerId);
		    messages = swallowBuffer.createMessageQueue(topicName, consumerId, messageIdOfTailMessage);
			freeChannelQueue = freeChannels.get(consumerId);
		}	
		
		try {
			while(true){
				Channel channel = null;
				synchronized(freeChannelQueue){
					if(freeChannelQueue == null){
						break;
					}
					channel = freeChannelQueue.poll(configManager.getFreeChannelBlockQueueOutTime(),TimeUnit.MILLISECONDS);
				}			
				if(channel == null){
					break;
					//TODO 用异常替代isConnected
				}else if(channel.isConnected()){
					if(preparedMesssages.get(consumerId) != null){
						pktMsg = preparedMesssages.get(consumerId);
						preparedMesssages.remove(consumerId);
					} else{					
						message = (SwallowMessage)messages.take();
						pktMsg = new PktObjectMessage(Destination.topic(topicName), message);
					}
					 Long messageId = message.getMessageId();
					//如果consumer是收到ACK之前更新messageId的类型
					 if(ConsumerType.UPDATE_BEFORE_ACK.equals(consumerTypes.get(consumerId))){
						 ackDao.add(topicName, consumerId, messageId);
					 }						 
					 while(true){
						 if(!channel.isWritable()){
							 if(channel.isConnected()){
								 Thread.sleep(1000);
								 continue;
							 } else {
								 preparedMesssages.put(consumerId, pktMsg);
								 break;
							 }								
						 } else{
								//TODO +isWritable?，连接断开后write后是否会抛异常，isWritable()=false的时候retry, when will write() throw exception?						
								channel.write(pktMsg);
							}
					 }
					
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private long getMessageIdOfTailMessage(String topicName, String consumerId) {
		
		Long maxMessageId = ackDao.getMaxMessageId(topicName, consumerId);
		if(maxMessageId == null){
			maxMessageId = messageDao.getMaxMessageId(topicName);
		}
		if(maxMessageId == null){
			int time = (int)(System.currentTimeMillis() / 1000);
			BSONTimestamp bst = new BSONTimestamp(time, 1);
			maxMessageId = MongoUtils.BSONTimestampToLong(bst);
		}
		return maxMessageId;
	}

	public void changeStatuesWhenChannelBreak(Channel channel){
		
		String consumerId = channelInformations.get(channel).getConsumerId();
		synchronized(channelWorkStatus){
			channelWorkStatus.get(consumerId).remove(channel);
		}
		synchronized(channelInformations){
			channelInformations.remove(channel);
		}
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
	
	public void checkMasterIsLive(final ServerBootstrap bootStrap){
		
		Runnable runnable = new Runnable() {

			@Override
			public void run() {								
				try {
					heartbeater.waitUntilBeginBeating(configManager.getMasterIp(), bootStrap, configManager.getHeartbeatCheckInterval(),configManager.getHeartbeatMaxStopTime());
				} catch (Exception e) {
					//log.error("Error update heart beat", e);
				}			
			}
		};
		Thread heartbeatThread = threadFactory.newThread(runnable, "checkMasterIsLive-");
		heartbeatThread.setDaemon(true);
		heartbeatThread.start();
	}
	
	public void handlePacket(final Channel channel, final Object consumerPacket){
		final String consumerId;
		final Destination  dest;
		final ConsumerType consumerType;
		ArrayBlockingQueue<Runnable> getAckWorker;
		//接收的包为greet信息时
		if(consumerPacket instanceof PktConsumerMessage){
			consumerId = ((PktConsumerMessage) consumerPacket).getConsumerId();
			dest = ((PktConsumerMessage) consumerPacket).getDest();
	    	consumerType = ((PktConsumerMessage) consumerPacket).getConsumerType();
	    	getAckWorker = getAckWorkers.get(consumerId);
	    	if(getAckWorker == null){
	    		getAckWorker = new ArrayBlockingQueue<Runnable>(10);//TODO 
	    	}
	    	getAckWorker.add(new Runnable() {
				@Override
				public void run() {					
			    	ChannelInformation channelInfo =  new ChannelInformation(dest, consumerId, consumerType);
			    	synchronized(channelInformations){
				    	channelInformations.put(channel, channelInfo);
			    	}
			    	changeChannelWorkStatus(consumerId, channel);		    	
					putChannelToBlockQueue(consumerId, channel);
					synchronized(getMessageThreadStatus){
						//TODO 不同topic的consumerId可以相同
						if(!getMessageThreadStatus.contains(consumerId)){
							addGetMessageThread(consumerId, dest.getName());
						}
					}					
				}
			});
	    	
	    	synchronized(consumerTypes){
	    		if(consumerTypes.get(consumerId) == null){
	        		consumerTypes.put(consumerId, consumerType);   		
	        		addHandleACKThread(consumerId, dest.getName(), getAckWorker);
	        	}
	    	}
		} else{
			//接收的包为ack时
			synchronized(channelInformations){
				ChannelInformation channelInfo =  channelInformations.get(channel);
				if(channelInfo == null){
					return;
				}
				consumerId = channelInfo.getConsumerId();
				dest = channelInfo.getDest();
			}			
			final Long messageId = null;//((PktConsumerACK)consumerPacket).getMessageId();
			getAckWorker = getAckWorkers.get(consumerId);
			getAckWorker.add(new Runnable() {
				@Override
				public void run() {								    	
					updateMaxMessageId(consumerId, dest.getName(), messageId);
					putChannelToBlockQueue(consumerId, channel);
				}
			});	
		}	
	}
}
