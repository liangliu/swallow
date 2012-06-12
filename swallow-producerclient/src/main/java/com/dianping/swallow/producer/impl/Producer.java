package com.dianping.swallow.producer.impl;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.TextMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;

public class Producer {
	private static Producer instance;
	private static int count;
	private static int ackNum;
	ApplicationContext ctx = new ClassPathXmlApplicationContext("spring-producerclient.xml");
	MQService swallowAgency = (MQService) ctx.getBean("server", MQService.class);

	private Producer(){
		count = 0;
	}
	public static synchronized Producer getInstance(){
		if(instance == null){
			instance = new Producer();
		}
		return instance;
	}
	
	public Packet send(String content){
		Destination dest = Destination.queue("master.slave");
		PktStringMessage strMsg = new PktStringMessage(dest, "U R a Little Pig", ++ackNum);
		return swallowAgency.sendMessage(strMsg);
	}
}
