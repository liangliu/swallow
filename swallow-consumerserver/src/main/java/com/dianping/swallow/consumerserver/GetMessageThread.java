package com.dianping.swallow.consumerserver;

import java.util.HashSet;
import org.jboss.netty.channel.Channel;
import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;

public class GetMessageThread implements Runnable{

	private CId2Topic cId2Topic;
	private Boolean isLive = true;
	private ConsumerServiceImpl cService;


	public void setcId2Topic(CId2Topic cId2Topic) {
		this.cId2Topic = cId2Topic;
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
			cService.pollFreeChannelsByCId(cId2Topic);
			synchronized(cService.getGetMessageThreadStatus()){
				HashSet<Channel> channels = cService.getChannelWorkStatus().get(cId2Topic);
				if(channels.isEmpty()){
					cService.getGetMessageThreadStatus().remove(cId2Topic);
					isLive = false;
				}
			}
		}
	}
		
}
