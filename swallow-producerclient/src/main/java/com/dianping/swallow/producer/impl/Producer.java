package com.dianping.swallow.producer.impl;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.packet.Message;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producer.HandlerUndeliverable;
import com.dianping.swallow.producer.ProducerType;

public class Producer {
	//变量定义
	private static Producer 		instance;				//Producer实例
	private ApplicationContext		ctx				= new ClassPathXmlApplicationContext("spring-producerclient.xml");//spring
	private MQService				swallowAgency	= (MQService) ctx.getBean("swallow", MQService.class);//获取Swallow代理
	private String					producerID		= UUID.randomUUID().toString();//Producer UUID
	private HandlerAsynchroMode		asyncHandler;			//异步处理对象
	private HandlerSynchroMode		syncHandler;			//同步处理对象
	private HandlerOneWayMode		onewayHandler;			//Oneway处理对象
	private HandlerUndeliverable	undeliverableMessageHandler;
	private String					filequeueName	= null;
	//常量定义
	public static final int			SENDER_NUM		= 10;//异步处理对象的线程池大小
	private static final Logger		log				= Logger.getLogger(Producer.class);
	private final ProducerType		producerType;
	private final String			producerVersion = "0.6.0"; 
	
	//构造函数
	private Producer(ProducerType producerType){
		this.producerType = producerType;
		//Producer工作模式
		switch(producerType){
		case SYNCHRO_MODE:
			syncHandler		= new HandlerSynchroMode(this);
			break;
		case ASYNCHRO_MODE:
			asyncHandler	= new HandlerAsynchroMode(this);
			break;
		case ONE_WAY_MODE:
			onewayHandler	= new HandlerOneWayMode(this);
			break;
		}
		//Message发送出错处理类
		undeliverableMessageHandler = new HandlerUndeliverable() {
			@Override
			public void handleUndeliverableMessage(Message msg) {
				// TODO Auto-generated method stub
				log.info("[Dest][" + msg.getDestination().getName() + "]" + msg.getContent());
			}
		};
		//向Swallow发送greet
		swallowAgency.sendMessageWithoutReturn(new PktProducerGreet(producerVersion));
	}
	
	//getters && setters
	//getInstance首次执行可指定Producer类型，再次指定Producer类型无效
	public static synchronized Producer getInstance(ProducerType producerType){
		if(instance == null)	instance = new Producer(producerType);
		return instance;
	}
	public String getProducerID() {
		return producerID;
	}
	public MQService getSwallowAgency() {
		return swallowAgency;
	}
	public String getFilequeueName() {
		return filequeueName;
	}
	public ProducerType getProducerType() {
		return producerType;
	}
	public String getProducerVersion() {
		return producerVersion;
	}
	public void setFilequeueName(String filequeueName) {
		this.filequeueName = filequeueName;
	}
	
	//发送指定Destination的Object，返回该Object的SHA-1字符串
	//异步模式和oneway模式返回值为null
	public String sendMessage(Destination dest, Object content){
		String ret = null;
		PktObjectMessage objMsg = new PktObjectMessage(dest, content);
		switch(producerType){
		case SYNCHRO_MODE://同步模式
			ret = ((PktSwallowPACK)syncHandler.doSendMsg(objMsg)).getShaInfo();
			if(ret == null){
				handleUndeliverableMessage(objMsg);
			}
			break;
		case ASYNCHRO_MODE://异步模式
			try {
				asyncHandler.doSendMsg(objMsg);
			} catch (FileQueueClosedException e) {
				log.info(e.toString());
				handleUndeliverableMessage(objMsg);
			}
			break;
		case ONE_WAY_MODE://OneWay模式
			onewayHandler.doSendMsg(objMsg);
			break;
		}
		return ret;
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