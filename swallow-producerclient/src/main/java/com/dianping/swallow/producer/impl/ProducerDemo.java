package com.dianping.swallow.producer.impl;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.TextMessage;

public class ProducerDemo {

	public static void main(String[] args){
		TextMessage strMsg = new TextMessage();
		strMsg.setContent("U R a little pig");
		ApplicationContext ctx = new ClassPathXmlApplicationContext("spring-producerclient.xml");
		ProducerServer ps = (ProducerServer) ctx.getBean("server", ProducerServer.class);
		System.out.println(ps.getStr(strMsg));
	}
}
