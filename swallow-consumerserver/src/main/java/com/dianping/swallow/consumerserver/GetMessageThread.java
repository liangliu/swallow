package com.dianping.swallow.consumerserver;

import java.io.Closeable;
import java.util.HashSet;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.consumerserver.impl.ConsumerServiceImpl;

public class GetMessageThread implements Runnable, Closeable{
	
	private static Logger LOG = LoggerFactory.getLogger(GetMessageThread.class);

	private CId2Topic cId2Topic;
	private volatile boolean isLive = true;
	private ConsumerServiceImpl cService;

	public void setcId2Topic(CId2Topic cId2Topic) {
		this.cId2Topic = cId2Topic;
	}

	public void setcService(ConsumerServiceImpl cService) {
		this.cService = cService;
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
			cService.sendMessageByPollFreeChannelQueue(cId2Topic);
			synchronized(cService.getGetMessageThreadStatus()){
				HashSet<Channel> channels = cService.getChannelWorkStatus().get(cId2Topic);
				if(channels.isEmpty()){
					cService.getGetMessageThreadStatus().remove(cId2Topic);
					isLive = false;
				}
			}
		}
		LOG.info("closed");
	}
		
}
