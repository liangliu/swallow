package com.dianping.swallow.consumerserver;

import java.util.concurrent.ArrayBlockingQueue;

import org.jboss.netty.channel.Channel;

public interface ConsumerService {

	/**
	 * 有新消息到的时候，往blockQueue中插入channel，表示此channel可接受消息。
	 * @param consumerId
	 * @param channel
	 */	
	//public void putChannelToBlockQueue(String consumerId, Channel channel);
	
	/**
	 * 有新消息到的时候，更新ChannelWorkStatus
	 * @param consumerId
	 * @param channel
	 */
	//public void changeChannelWorkStatus(String consumerId, Channel channel);
	
	/**
	 * @param consumerId
	 * @param topicName
	 * @param getAckWorker
	 * 有新的channel连接时，对于存在同consumerId的线程，不做处理；否则新增线程。
	 */	
	//public void addThread(String consumerId, String topicName, ArrayBlockingQueue<Runnable> getAckWorker);
	
	/**
	 * 轮询对应consumerId的blockQueue中的channel
	 * @param consumerId
	 * @param topicId
	 */
	//public void pollFreeChannelsByCId(String consumerId,String topicId);
	
	/**
	 * 当channel断开时做的处理
	 * @param channel
	 */
	//public void changeStatuesWhenChannelBreak(Channel channel);
}
