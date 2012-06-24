package com.dianping.swallow.producer.impl;

import com.dianping.swallow.common.packet.Packet;

public class HandlerSynchroMode {
	private ProducerImpl producer;
	public HandlerSynchroMode(ProducerImpl producer){
		this.producer = producer;
	}
	//对外接口
	public Packet doSendMsg(Packet pkt){
		return producer.getRemoteService().sendMessage(pkt);
	}
}
