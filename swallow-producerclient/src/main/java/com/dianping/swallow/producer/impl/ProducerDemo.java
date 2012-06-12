package com.dianping.swallow.producer.impl;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.TextMessage;
import com.dianping.swallow.common.util.MQService;

public class ProducerDemo {

	public static void main(String[] args){
		TextMessage strMsg = new TextMessage();
		strMsg.setContent("U R a little pig");
		ApplicationContext ctx = new ClassPathXmlApplicationContext("spring-producerclient.xml");
		MQService ps = (MQService) ctx.getBean("server", MQService.class);
//		System.out.println(ps.sendMessage(strMsg));
	}
}
