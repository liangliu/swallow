package com.dianping.swallow.producer.impl;

import com.dianping.swallow.common.packet.Packet;

public class HandlerSynchroMode {
	private Producer producer;
	public HandlerSynchroMode(Producer producer){
		this.producer = producer;
	}
	public Packet doSendMsg(Packet pkt){
		return producer.getSwallowAgency().sendMessage(pkt);
	}
}
