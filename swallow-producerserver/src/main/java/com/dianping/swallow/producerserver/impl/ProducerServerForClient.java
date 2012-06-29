package com.dianping.swallow.producerserver.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dianping.dpsf.api.ServiceRegistry;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.util.IPUtil;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServerForClient implements MQService {

   private static final Logger logger           = Logger.getLogger(ProducerServerForClient.class);
   private static final int    DEFAULT_PORT     = 4000;
   public static final String  producerServerIP = IPUtil.getFirstNoLoopbackIP4Address();

   private int                 port             = DEFAULT_PORT;
   private MessageDAO          messageDAO;

   /**
    * 启动producerServerClient
    * 
    * @param port 供producer连接的端口
    * @throws RemoteServiceInitFailedException 远程调用初始化失败
    * @throws Exception 连续绑定同一个端口抛出异常，pigeon初始化失败抛出异常
    */
   public void start() throws RemoteServiceInitFailedException {
      try {
         ServiceRegistry remoteService = null;
         remoteService = new ServiceRegistry(getPort());
         Map<String, Object> services = new HashMap<String, Object>();
         services.put("remoteService", this);
         remoteService.setServices(services);
         remoteService.init();
         logger.info("[ProducerServerForClient]:[Initialize pigeon sucessfully.]");
      } catch (Exception e) {
         logger.error("[ProducerServerForClient]:[Initialize pigeon failed.]", e);
         throw new RemoteServiceInitFailedException();
      }
   }

   /**
    * 保存swallowMessage到数据库
    * 
    * @throws ServerDaoException
    */
   @Override
   public Packet sendMessage(Packet pkt) throws ServerDaoException {
      Packet pktRet = null;
      SwallowMessage swallowMessage;
      String topicName;
      String sha1;
      switch (pkt.getPacketType()) {
         case PRODUCER_GREET:
            logger.info("[ProducerServerForClient]:[Got Greet][From=" + ((PktProducerGreet) pkt).getProducerIP()
                  + "][Version=" + ((PktProducerGreet) pkt).getProducerVersion() + "]");
            //返回ProducerServer地址
            pktRet = new PktSwallowPACK(producerServerIP);
            break;
         case OBJECT_MSG:
            swallowMessage = ((PktMessage) pkt).getContent();
            topicName = ((PktMessage) pkt).getDestination().getName();
            sha1 = SHAGenerater.generateSHA(swallowMessage.getContent());
            pktRet = new PktSwallowPACK(sha1);
            //设置swallowMessage的sha-1
            swallowMessage.setSha1(sha1);

            //将swallowMessage保存到mongodb
            try {
               messageDAO.saveMessage(topicName, swallowMessage);
            } catch (Exception e) {
               logger.error("[ProducerServerForClient]:[Save message to DB failed.]", e);
               throw new ServerDaoException();
            }
            break;
         default:
            logger.warn("[ProducerServerForClient]:[Received unrecognized packet.]");
            break;
      }
      return pktRet;
   }

   public int getPort() {
      return port;
   }

   public void setPort(int port) {
      this.port = port;
   }

   public void setMessageDAO(MessageDAO messageDAO) {
      this.messageDAO = messageDAO;
   }
}
