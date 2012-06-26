package com.dianping.swallow.consumerserver;

import java.util.HashSet;
import org.jboss.netty.channel.Channel;
import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;

public class GetMessageThread implements Runnable{
	private String topicName;
	private String consumerId;
	private Boolean isLive = true;
	private ConsumerServiceImpl cService;
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
		try {
			Thread.sleep(1000);//TODO
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while(isLive){
			cService.pollFreeChannelsByCId(consumerId, topicName);
			synchronized(cService.getConsumerTypes()){
				HashSet<Channel> channels = cService.getChannelWorkStatus().get(consumerId);
				if(channels.isEmpty()){
					cService.getConsumerTypes().remove(consumerId);
					isLive = false;
				}
			}
		}
	}
		
}
