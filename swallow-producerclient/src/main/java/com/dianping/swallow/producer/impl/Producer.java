package com.dianping.swallow.producer.impl;

import java.util.UUID;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;

public class Producer {
	//变量定义
	private static Producer 	instance;				//Producer实例
	private Destination			defaultDest		= Destination.queue("master.slave");
	private ApplicationContext	ctx				= new ClassPathXmlApplicationContext("spring-producerclient.xml");//spring
	private MQService			swallowAgency	= (MQService) ctx.getBean("server", MQService.class);//获取Swallow代理
	private boolean				synchroMode		= false;	//是否同步
	private String				producerID		= UUID.randomUUID().toString();//Producer UUID
	private HandlerAsynchroMode	asyncHandler;	//异步处理对象
	private HandlerSynchroMode	syncHandler;	//同步处理对象
	//常量定义
	public static final int		SENDER_NUM		= 10;//异步处理对象的线程池大小
	//构造函数
	private Producer(){
		//Producer异步模式
		if(synchroMode){
			syncHandler		= new HandlerSynchroMode(this);
		}else{
			asyncHandler	= new HandlerAsynchroMode(this);
		}
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
	
	public String sendString(Destination dest, String content){
		String ret = null;
		PktStringMessage strMsg = new PktStringMessage(dest, content);
		if(synchroMode){
			ret = ((PktSwallowPACK)syncHandler.doSendMsg(strMsg)).getShaInfo();
		}else{
			try {
				asyncHandler.doSendMsg(strMsg);
			} catch (FileQueueClosedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ret;
	}
	public String sendString(String content){
		return sendString(defaultDest,content);
	}
	public String sendMsg(Object obj){
		String ret = null;
		return ret;
	}
}
