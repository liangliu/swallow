package com.dianping.swallow.consumerserver.impl;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.internal.dao.HeartbeatDAO;
import com.dianping.swallow.consumerserver.Heartbeater;

public class MongoHeartbeater implements Heartbeater {

   private static final Logger LOG        = LoggerFactory.getLogger(MongoHeartbeater.class);
   private HeartbeatDAO  heartbeatDAO;

   public void setHeartbeatDAO(HeartbeatDAO heartbeatDAO) {
      this.heartbeatDAO = heartbeatDAO;
   }

   @Override
   public void beat(String ip) {
      heartbeatDAO.updateLastHeartbeat(ip);
   }

   @Override
   public void waitUntilMasterDown(String ip, long checkInterval, long maxStopTime) throws InterruptedException {
      long startTime = System.currentTimeMillis();
      long lastBeatTime = startTime;
      while (true) {
         Date beat = null;
         try {
            beat = heartbeatDAO.findLastHeartbeat(ip);
         } catch (Exception e) {
            //如果访问mongo出错，重置startTime，防止failover时间过长
            LOG.error("error find last heartbeat", e);
            startTime = System.currentTimeMillis();
            Thread.sleep(1000);
            continue;
         }
         if (beat == null) {
            LOG.info(ip + " no beat");
            if (System.currentTimeMillis() - startTime > maxStopTime) {
               break;
            }
         } else {
            LOG.info(ip + " beat at " + beat.getTime());
            long now = System.currentTimeMillis();
            lastBeatTime = beat.getTime();
            if (now - lastBeatTime > maxStopTime) {
               break;
            }
         }
         Thread.sleep(checkInterval);
      }
      LOG.info(ip + " master stop beating, slave start");
   }

   @Override
   public void waitUntilMasterUp(String ip, long checkInterval, long maxStopTime)
         throws InterruptedException {
      Date beat = null;
      while (true) {
         try {
            beat = heartbeatDAO.findLastHeartbeat(ip);
         } catch (Exception e) {
            LOG.error("error find last heartbeat", e);
            Thread.sleep(1000);
            continue;
         }
         if (beat != null) {
            long lastBeatTime = beat.getTime();
            long now = System.currentTimeMillis();
            if (now - lastBeatTime < maxStopTime) {
               //bootStrap.releaseExternalResources();
               break;
            }
         }
         Thread.sleep(checkInterval);
      }

   }

}
