package com.dianping.swallow.consumerserver.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.cat.CatMonitorBean;
import com.dianping.swallow.common.consumer.ACKHandlerType;
import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.threadfactory.MQThreadFactory;
import com.dianping.swallow.consumerserver.Heartbeater;
import com.dianping.swallow.consumerserver.buffer.SwallowBuffer;
import com.dianping.swallow.consumerserver.config.ConfigManager;

public class ConsumerWorkerManager implements CatMonitorBean {

   private static final Logger             logger                    = LoggerFactory
                                                                           .getLogger(ConsumerWorkerManager.class
                                                                                 .getName());

   private AckDAO                          ackDAO;
   private Heartbeater                     heartbeater;
   private SwallowBuffer                   swallowBuffer;
   private MessageDAO                      messageDAO;

   private ConfigManager                   configManager             = ConfigManager.getInstance();

   private MQThreadFactory                 threadFactory             = new MQThreadFactory();

   private Map<ConsumerId, ConsumerWorker> consumerId2ConsumerWorker = new ConcurrentHashMap<ConsumerId, ConsumerWorker>();

   public void setAckDAO(AckDAO ackDAO) {
      this.ackDAO = ackDAO;
   }

   public void setHeartbeater(Heartbeater heartbeater) {
      this.heartbeater = heartbeater;
   }

   public void setSwallowBuffer(SwallowBuffer swallowBuffer) {
      this.swallowBuffer = swallowBuffer;
   }

   public void setMessageDAO(MessageDAO messageDAO) {
      this.messageDAO = messageDAO;
   }

   public ConfigManager getConfigManager() {
      return configManager;
   }

   public void handleGreet(Channel channel, ConsumerInfo consumerInfo, int clientThreadCount) {
      findOrCreateConsumerWorker(consumerInfo).handleGreet(channel, clientThreadCount);
   }

   public void handleAck(Channel channel, ConsumerInfo consumerInfo, Long ackedMsgId, ACKHandlerType type) {
      findOrCreateConsumerWorker(consumerInfo).handleAck(channel, ackedMsgId, type);
   }

   public void handleChannelDisconnect(Channel channel, ConsumerInfo consumerInfo) {
      findOrCreateConsumerWorker(consumerInfo).handleChannelDisconnect(channel);
   }

   public void close() throws IOException {
      for (Map.Entry<ConsumerId, ConsumerWorker> entry : consumerId2ConsumerWorker.entrySet()) {
         entry.getValue().close();
      }
   }

   private ConsumerWorker findOrCreateConsumerWorker(ConsumerInfo consumerInfo) {
      ConsumerId consumerId = consumerInfo.getConsumerId();
      ConsumerWorker consumerWorker = consumerId2ConsumerWorker.get(consumerId);
      if (consumerWorker == null) {
         synchronized (this) {
            if (consumerWorker == null) {
               consumerWorker = new ConsumerWorkerImpl(consumerInfo, this);
               consumerId2ConsumerWorker.put(consumerId, consumerWorker);
            }
         }
      }
      return consumerWorker;
   }

   public void init(boolean isSlave) {
      if (isSlave) {
         try {
            // wont throw MongoException
            heartbeater.waitUntilStopBeating(configManager.getMasterIp(), configManager.getHeartbeatCheckInterval(),
                  configManager.getHeartbeatMaxStopTime());
         } catch (InterruptedException e) {
            return;
         }
      } else {
         startHeartbeater(configManager.getMasterIp());
      }

   }

   private void startHeartbeater(final String ip) {

      Runnable runnable = new Runnable() {

         @Override
         public void run() {
            while (true) {

               try {
                  heartbeater.beat(ip);
                  Thread.sleep(configManager.getHeartbeatUpdateInterval());
               } catch (Exception e) {
                  logger.error("Error update heart beat", e);
               }
            }
         }

      };

      Thread heartbeatThread = threadFactory.newThread(runnable, "heartbeat-");
      heartbeatThread.setDaemon(true);
      heartbeatThread.start();
   }

   public void checkMasterIsLive(final ServerBootstrap bootStrap) {

      try {
         heartbeater.waitUntilBeginBeating(configManager.getMasterIp(), bootStrap,
               configManager.getHeartbeatCheckInterval(), configManager.getHeartbeatMaxStopTime());
      } catch (Exception e) {
         //log.error("Error update heart beat", e);
      }

   }

   @Override
   public Map<String, Object> getStatusMap() {
      Map<String, Object> map = new HashMap<String, Object>(1);
      map.put("consumerIds", this.consumerId2ConsumerWorker.keySet());
      return map;
   }
   
   public void workerDone(ConsumerId consumerId) {
	   consumerId2ConsumerWorker.remove(consumerId);
   }

	public AckDAO getAckDAO() {
		return ackDAO;
	}

	public SwallowBuffer getSwallowBuffer() {
		return swallowBuffer;
	}

	public MessageDAO getMessageDAO() {
		return messageDAO;
	}

	public MQThreadFactory getThreadFactory() {
		return threadFactory;
	}
   
}
