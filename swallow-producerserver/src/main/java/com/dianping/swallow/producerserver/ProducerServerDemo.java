package com.dianping.swallow.producerserver;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.swallow.common.message.TextMessage;

public class ProducerServerDemo implements ProducerServer {
	private String str;
	public String getStr(){
		return str;
	}
	public void setStr(String str) {
		this.str = str;
	}
	public static void main(String[] args) {
		new ClassPathXmlApplicationContext(new String[]{"spring-producerserver.xml"});
	}
	@Override
	public String getStr(TextMessage strMsg) {
		// TODO Auto-generated method stub
		System.out.println(strMsg.getContent());
		return getStr();
	}	
}
