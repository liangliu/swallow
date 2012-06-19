package com.dianping.swallow.producerserver.impl;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServer implements MQService {

	private static Map<Destination, InetSocketAddress> DestinationAndDBMap = new HashMap<Destination, InetSocketAddress>();
//	private MongoClient mongoClient;
	
	public ProducerServer(){
		new ProducerServerText(this).start();
	}
	
	//Lion的Destination-DB映射（DDMap）配置更新
	private void onDDMapChanged(){
		
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

	public static void main(String[] args) {
		new ClassPathXmlApplicationContext(new String[]{"spring-producerserver.xml"});
	}
}
