package com.dianping.swallow.producerserver.impl;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PacketType;
import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.MQService;

public class ProducerServer implements MQService {
	private String str;
	public String getStr(){
		return str;
	}
	public void setStr(String str) {
		this.str = str;
	}

	@Override
	public Packet sendMessage(Packet pkt) {
		// TODO Auto-generated method stub
		Packet pktRet = null;
		switch(pkt.getPacketType()){
		case PRODUCER_GREET:
			pktRet = new PktSwallowPACK(-1);
			break;
		case STRING_MSG:
			pktRet = new PktSwallowPACK(((PktStringMessage)pkt).getAckNum());
			System.out.println((PktStringMessage)pkt);
			break;
		case BINARY_MSG:
			break;
		default:
			break;
		}
		return pktRet;
	}

	public static void main(String[] args) {
		new ClassPathXmlApplicationContext(new String[]{"spring-producerserver.xml"});
	}
}
