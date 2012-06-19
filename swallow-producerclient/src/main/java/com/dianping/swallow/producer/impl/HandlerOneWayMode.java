package com.dianping.swallow.producer.impl;

import com.dianping.swallow.common.packet.Packet;

public class HandlerOneWayMode {
	Producer producer;
	public HandlerOneWayMode(Producer producer){
		this.producer = producer;
	}
	public void doSendMsg(Packet pkt){
		producer.getSwallowAgency().sendMessageWithoutReturn(pkt);
	}
}
