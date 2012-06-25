package com.dianping.swallow.consumerserver;

import java.util.HashSet;

import org.jboss.netty.channel.Channel;

import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;

public class GetMessageThread implements Runnable{
	private String topicName;
	private String consumerId;
	private String preparedMes = null;
	private String message = null;
	private Boolean isLive = true;
	private ConsumerServiceImpl cService;
	private AckDAO dao;
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public void setcService(ConsumerServiceImpl cService) {
		this.cService = cService;
	}

	@Override
	public void run() {
		//TODO 获取mongo中最大的timeStamp，然后blockingQueue去获取！
		while(isLive){
			cService.ergodicChannelByCId(consumerId, topicName);
			synchronized(cService.getThreads()){
				HashSet<Channel> channels = cService.getChannelWorkStatue().get(consumerId);
				//TODO 貌似是不需要同步！
				if(channels.isEmpty()){
					cService.getThreads().remove(consumerId);
					isLive = false;
				}
			}
		}		
		//maxTStamp = dao.getMaxTimeStamp(topicId, consumerId);
		
		//TODO refactor to ConsumerService
//		HashMap<Channel, String> channels = ConsumerService.cService.getChannelWorkStatue().get(consumerId);		
//		while(isLive){
//		
//			try {
//				//处理完一次，线程睡眠，然后继续执行。
//				//TODO 接收到ack的时候触发发送线程工作
//				Thread.sleep(ConsumerService.cService.getConfigManager().getPullingTime());
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
	}
		
}
