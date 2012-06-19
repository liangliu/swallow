package com.dianping.swallow.producerserver.impl;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServer implements MQService {

	public ProducerServer(){
		new ProducerServerText(this).start();
		this.start();
	}
	
	@Override
	public void sendMessageWithoutReturn(Packet pkt) {
		sendMessage(pkt);
	}

	@Override
	public Packet sendMessage(Packet pkt) {
		Packet pktRet = null;
		switch(pkt.getPacketType()){
		case PRODUCER_GREET:
			pktRet = new PktSwallowPACK(SHAGenerater.generateSHA(((PktProducerGreet)pkt).getProducerVersion()));
			break;
		case OBJECT_MSG:
			pktRet = new PktSwallowPACK(SHAGenerater.generateSHA(((PktObjectMessage)pkt).getContent()));
			System.out.println("Got ObjectMessage. " + (String)((PktObjectMessage)pkt).getContent());
			//TODO: DAO
			break;
		case TEXT_MSG:
			pktRet = new PktSwallowPACK(SHAGenerater.generateSHA(((PktTextMessage)pkt).getContent()));
			//TODO: DAO
			break;
		default:
			break;
		}
		return pktRet;
	}
	
	public void start(){
		new ClassPathXmlApplicationContext(new String[]{"spring-producerserver.xml"});
	}
}