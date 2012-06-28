package com.dianping.swallow.consumerserver;

import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;

import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;

public class HandleACKThread implements Runnable{
	
	private ArrayBlockingQueue<Runnable> getAckWorker;
	
	private CId2Topic cId2Topic;
	
	private ConsumerServiceImpl cService;
	
	private Boolean isLive = true;


	public void setcId2Topic(CId2Topic cId2Topic) {
		this.cId2Topic = cId2Topic;
	}

	public void setcService(ConsumerServiceImpl cService) {
		this.cService = cService;
	}

	public void setGetAckWorker(ArrayBlockingQueue<Runnable> getAckWorker) {
		this.getAckWorker = getAckWorker;
	}

	@Override
	public void run() {
		while(isLive){
			Runnable worker = null;
			try {
				while(true){
					worker = getAckWorker.poll(cService.getConfigManager().getFreeChannelBlockQueueOutTime(),TimeUnit.MILLISECONDS);
					if(worker != null){
						worker.run();
					} else{
						break;
					}
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			synchronized(cService.getConsumerTypes()){
				HashSet<Channel> channels = cService.getChannelWorkStatus().get(cId2Topic);
				if(channels.isEmpty()){
					cService.getConsumerTypes().remove(cId2Topic);
					isLive = false;
				}
			}
		}
		
	}

}
