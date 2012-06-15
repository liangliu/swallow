package com.dianping.swallow.consumerserver;

import org.jboss.netty.channel.Channel;

public interface ConsumerService {

	/**
	 * 有新消息到的时候，更新channel的状态
	 * @param consumerId
	 * @param channel
	 */
	public void updateChannelWorkStatues(String consumerId, Channel channel);
	
	/**
	 * @param consumerId
	 * @param topicId
	 * 有新的channel连接时，对于存在同consumerId的线程，不做处理；否则新增线程。
	 */	
	public void updateThreadWorkStatues(String consumerId, String topicId);
	
	/**
	 * 遍历一遍同consumerId下所有的channel
	 * @param consumerId
	 * @param topicId
	 * @param isLive
	 */
	public void ergodicChannelByCId(String consumerId,String topicId, Boolean isLive);
	
	/**
	 * 当channel断开时做的处理
	 * @param channel
	 */
	public void changeStatuesWhenChannelBreak(Channel channel);
}
