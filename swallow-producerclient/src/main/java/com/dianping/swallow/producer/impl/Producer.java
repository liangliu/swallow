package com.dianping.swallow.producer.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.dpsf.spring.ProxyBeanFactory;
import com.dianping.filequeue.DefaultFileQueueImpl;
import com.dianping.filequeue.FileQueue;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;

public class Producer {
	//变量定义
	private static Producer 	instance;//Producer实例
	private Destination 		dest	= null;//Producer目标topic，未设置Destination时从Lion获取默认值
	private ApplicationContext	ctx		= new ClassPathXmlApplicationContext("spring-producerclient.xml");//spring
	private MQService			swallowAgency	= (MQService) ctx.getBean("server", MQService.class);//获取Swallow代理
	private boolean				isSynchro		= true;//是否同步
	private ExecutorService		sender			= Executors.newCachedThreadPool();
	private FileQueue<Packet>	messageQueue	= new DefaultFileQueueImpl<Packet>("filequeue.properties", "aa");
	//构造函数
	private Producer(){
		dest = null;//设置默认Destination//正式版本将从Lion获取
		if(!isSynchro){
			
		}
	}
	//getters && setters
	public Destination getDestination() {
		return dest;
	}
	public void setDestination(Destination dest) {
		this.dest = dest;
	}
	public static synchronized Producer getInstance(){
		if(instance == null){
			instance = new Producer();
		}
		return instance;
	}
	
	//TODO 返回UUID
	public Packet send(String content){
		Destination dest = Destination.queue("master.slave");
		PktStringMessage strMsg = new PktStringMessage(dest, "U R a Little Pig");
		return swallowAgency.sendMessage(strMsg);
	}
	
	private void sendMessageQueue(){
		
	}
}
