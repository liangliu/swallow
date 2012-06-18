package com.dianping.swallow.producer.impl;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.packet.Message;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producer.HandlerUndeliverable;

public class Producer {
	//变量定义
	private static Producer 		instance;				//Producer实例
	private Destination				defaultDest		= Destination.queue("master.slave");
	private ApplicationContext		ctx				= new ClassPathXmlApplicationContext("spring-producerclient.xml");//spring
	private MQService				swallowAgency	= (MQService) ctx.getBean("server", MQService.class);//获取Swallow代理
	private boolean					synchroMode		= true;	//是否同步
	private String					producerID		= UUID.randomUUID().toString();//Producer UUID
	private HandlerAsynchroMode		asyncHandler;			//异步处理对象
	private HandlerSynchroMode		syncHandler;			//同步处理对象
	private HandlerUndeliverable	undeliverableMessageHandler;
	private String					filequeueName	= null;
	
	//常量定义
	public static final int			SENDER_NUM		= 10;//异步处理对象的线程池大小
	private static final Logger		log				= Logger.getLogger(Producer.class);
	
	//构造函数
	//异步模式
	private Producer(String filequeueName){
		//Producer工作模式
		if(synchroMode){
			syncHandler		= new HandlerSynchroMode(this);
		}else{
			asyncHandler	= new HandlerAsynchroMode(this);
		}
		//Message发送出错处理类
		undeliverableMessageHandler = new HandlerUndeliverable() {
			@Override
			public void handleUndeliverableMessage(Message msg) {
				// TODO Auto-generated method stub
				log.info("[Dest][" + msg.getDestination().getName() + "]" + msg.getContent());
			}
		};
	}
	//同步模式
	private Producer(){
		
	}
	
	//getters && setters
	public Destination getDefaultDestination() {
		return defaultDest;
	}
	public void setDefaultDestination(Destination defaultDest) {
		this.defaultDest = defaultDest;
	}
	public String getProducerID() {
		return producerID;
	}
	public MQService getSwallowAgency() {
		return swallowAgency;
	}
	public static synchronized Producer getInstance(){
		if(instance == null)	instance = new Producer();
		return instance;
	}
	public String getFilequeueName() {
		return filequeueName;
	}
	public void setFilequeueName(String filequeueName) {
		this.filequeueName = filequeueName;
	}
	
	//发送指定Destination的Object//TODO: 使用序列化
	public String sendMessage(Destination dest, Object content){
		String ret = null;
		PktObjectMessage objMsg = new PktObjectMessage(dest, content);
		if(synchroMode){
			ret = ((PktSwallowPACK)syncHandler.doSendMsg(objMsg)).getShaInfo();
			if(ret == null){
				handleUndeliverableMessage(objMsg);
			}
		}else{
			try {
				asyncHandler.doSendMsg(objMsg);
			} catch (FileQueueClosedException e) {
				e.printStackTrace();
				handleUndeliverableMessage(objMsg);
			}
		}
		return ret;
	}
	//发生送默认Destination的Object
	public String sendMessage(Object content){
		return sendMessage(defaultDest, content);
	}
	//处理发送失败的Handle
	private void handleUndeliverableMessage(Message msg) {
		try {
			undeliverableMessageHandler.handleUndeliverableMessage(msg);
		} catch (Exception e) {
			log.error("error processing undeliverable message", e);
		}
	}
}