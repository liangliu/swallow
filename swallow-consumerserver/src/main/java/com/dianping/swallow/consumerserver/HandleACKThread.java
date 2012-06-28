package com.dianping.swallow.consumerserver;

import java.io.Closeable;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;

public class HandleACKThread implements Runnable, Closeable{
	
	private static Logger LOG = LoggerFactory.getLogger(HandleACKThread.class);
	
	private ArrayBlockingQueue<Runnable> getAckWorker;
	
	private CId2Topic cId2Topic;
	
	private ConsumerServiceImpl cService;
	
	private volatile boolean isLive = true;


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
	public void close() {
		LOG.info("receive close command");
		LOG.info("closing");
		isLive = false;
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
				LOG.error("unexpected interrupt", e);
			}			
			synchronized(cService.getConsumerTypes()){
				HashSet<Channel> channels = cService.getChannelWorkStatus().get(cId2Topic);
				if(channels.isEmpty()){
					cService.getConsumerTypes().remove(cId2Topic);
					isLive = false;
				}
			}
		}
		LOG.info("closed");
	}

}
