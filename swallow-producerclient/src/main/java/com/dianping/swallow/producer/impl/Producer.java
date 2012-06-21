package com.dianping.swallow.producer.impl;

import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Message;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producer.HandlerUndeliverable;
import com.dianping.swallow.producer.ProducerMode;

public class Producer {
	//变量定义
	private static Producer 		instance;				//Producer实例
	private ApplicationContext		ctx				= new ClassPathXmlApplicationContext("spring-producerclient.xml");//spring
	private MQService				swallowAgency	= (MQService) ctx.getBean("swallow", MQService.class);//获取Swallow代理
	private HandlerAsynchroMode		asyncHandler;			//异步处理对象
	private HandlerSynchroMode		syncHandler;			//同步处理对象
	private HandlerOneWayMode		onewayHandler;			//Oneway处理对象
	private HandlerUndeliverable	undeliverableMessageHandler;
	//常量定义
	private final String			producerVersion = "0.6.0";
	private final Logger			log				= Logger.getLogger(Producer.class);

	private final ProducerMode		producerMode;			//Producer工作模式
	private final Destination		destination;			//Producer消息目的
	private	final int				senderNum;				//异步处理对象的线程池大小
	
	private void init(){
		System.out.println("Inside init() method.");
	}
	
	//构造函数
	private Producer(ProducerMode producerType, Destination destination){
		this.producerMode	= producerType;
		this.destination	= destination;
		this.senderNum		= 10;
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
				log.info("[Dest][" + msg.getDestination().getName() + "]" + msg.getContent());
			}
		};
		//向Swallow发送greet
		swallowAgency.sendMessageWithoutReturn(new PktProducerGreet(producerVersion));
	}
	
	//getInstance首次执行可指定Producer类型，再次指定Producer类型无效
	public static synchronized Producer getInstance(ProducerMode producerMode, Destination destination){
		if(instance == null)	instance = new Producer(producerMode, destination);
		return instance;
	}

	//发送指定Destination的Object，返回该Object的SHA-1字符串
	//异步模式和oneway模式返回值为null
	public String sendMessage(Object content){
		return sendMessage(content, null);
	}
	
	public String sendMessage(Object content, Properties properties){
		String ret = null;

		//根据content生成SwallowMessage
		SwallowMessage swallowMsg = new SwallowMessage();
		
		swallowMsg.setContent(content);
		swallowMsg.setVersion(producerVersion);
		swallowMsg.setGeneratedTime(new Date());
//		if(properties != null)	swallowMsg.setProperties(properties);
		
		//构造packet
		PktObjectMessage objMsg = new PktObjectMessage(destination, swallowMsg);
		//TODO pigeon是否支持同一个连接既有oneway又有sync
		switch(producerMode){
		case SYNCHRO_MODE://同步模式
			ret = ((PktSwallowPACK)syncHandler.doSendMsg(objMsg)).getShaInfo();
			if(ret == null)	handleUndeliverableMessage(objMsg);
			break;
		case ASYNCHRO_MODE://异步模式
			try {
				asyncHandler.doSendMsg(objMsg);
			} catch (FileQueueClosedException fqce) {
				log.info(fqce.toString());
				handleUndeliverableMessage(objMsg);
			}
			break;
		//TODO 不用了
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
	
	//getters && setters
	public MQService getSwallowAgency() {
		return swallowAgency;
	}
	
	public ProducerMode getProducerType() {
		return producerMode;
	}
	
	public String getProducerVersion() {
		return producerVersion;
	}
	
	public Destination getDestination() {
		return destination;
	}

	public int getSenderNum() {
		return senderNum;
	}
	
}