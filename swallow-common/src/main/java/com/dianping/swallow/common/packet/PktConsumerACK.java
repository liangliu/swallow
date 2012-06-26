/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-25
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.common.packet;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;



/**
 * 
 * @author yu.zhang
 *
 */
public final class PktConsumerACK extends Packet {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8966695130892127961L;
	
	//TODO 增加一个greetMessage, ack只需要messageId，channel状态在swallowC保存

	private Destination dest;
	
	private String consumerId;
	
	private Long messageId;
	
	private ConsumerType consumerType;
	

	public ConsumerType getConsumerType() {
		return consumerType;
	}
	public void setConsumerType(ConsumerType consumerType) {
		this.consumerType = consumerType;
	}
	public Destination getDest() {
		return dest;
	}
	public void setDest(Destination dest) {
		this.dest = dest;
	}
	public String getConsumerId() {
		return consumerId;
	}
	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public Long getMessageId() {
		return messageId;
	}
	public void setMessageId(Long messageId) {
		this.messageId = messageId;
	}
	public PktConsumerACK(String consumerId, Destination dest, ConsumerType consumerType, Long messageId){
		this.setPacketType(PacketType.CONSUMER_ACK);
		this.dest = dest;
		this.consumerId = consumerId;
		this.messageId = messageId;
		this.consumerType = consumerType;
	}
	
}
