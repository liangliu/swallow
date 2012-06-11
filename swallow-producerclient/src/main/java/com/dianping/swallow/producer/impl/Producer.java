package com.dianping.swallow.producer.impl;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.TextMessage;

public class Producer {
	private static Producer instance;
	private static int count;
	ApplicationContext ctx = new ClassPathXmlApplicationContext("spring-producerclient.xml");
	ProducerServer swallowAgency = (ProducerServer) ctx.getBean("server", ProducerServer.class);

	private Producer(){
		count = 0;
	}
	public static synchronized Producer getInstance(){
		if(instance == null){
			instance = new Producer();
		}
		return instance;
	}
	
	public String send(String content){
		TextMessage txtMsg = new TextMessage();
		txtMsg.setContent(content);
		return swallowAgency.getStr(txtMsg);
	}
	public static synchronized void setOptions(){
		
	}
}
