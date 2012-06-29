package com.dianping.swallow.consumerserver.impl;



import java.util.Date;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import com.dianping.swallow.common.dao.HeartbeatDAO;
import com.dianping.swallow.consumerserver.Heartbeater;
import com.dianping.swallow.consumerserver.bootstrap.SlaveBootStrap;

public class MongoHeartbeater implements Heartbeater {

	private static Logger log = Logger.getLogger(MongoHeartbeater.class);

	private HeartbeatDAO heartbeatDAO;

	public void setHeartbeatDAO(HeartbeatDAO heartbeatDAO) {
		this.heartbeatDAO = heartbeatDAO;
	}

	@Override
	public void beat(String ip) {
		heartbeatDAO.updateLastHeartbeat(ip);
	}

	@Override
	public void waitUntilStopBeating(String ip, long checkInterval, long maxStopTime) throws InterruptedException {
		long startTime = System.currentTimeMillis();
		long lastBeatTime = startTime;
		while (true) {
			Date beat = null;
			try {
				beat = heartbeatDAO.findLastHeartbeat(ip);
			} catch (Exception e) {
				//如果访问mongo出错，重置startTime，防止failover时间过长
				log.error("error find last heartbeat", e);
				startTime = System.currentTimeMillis();
				Thread.sleep(1000);
				continue;
			}
			if (beat == null) {
				log.info(ip + " no beat");
				if (System.currentTimeMillis() - startTime > maxStopTime) {
					break;
				}
			} else {
				log.info(ip + " beat at " + beat.getTime());
				long now = System.currentTimeMillis();
				lastBeatTime = beat.getTime();
				if (now - lastBeatTime > maxStopTime) {
					break;
				}
			}
			Thread.sleep(checkInterval);
		}
		log.info(ip + " master stop beating, slave start");
	}

	@Override
	public void waitUntilBeginBeating(String ip, ServerBootstrap bootStrap, long checkInterval, long maxStopTime) throws InterruptedException {
		Date beat = null;
		while (true) {
			try {
				beat = heartbeatDAO.findLastHeartbeat(ip);
			} catch (Exception e) {
				log.error("error find last heartbeat", e);
				Thread.sleep(1000);
				continue;
			}
			if(beat != null){
				long lastBeatTime = beat.getTime();
				long now = System.currentTimeMillis();
				if (now - lastBeatTime < maxStopTime) {
					bootStrap.releaseExternalResources();
					SlaveBootStrap.slaveShouldLive = Boolean.FALSE;
					break;
				}				
			}
			Thread.sleep(checkInterval);
		}
		
		
	}

}
