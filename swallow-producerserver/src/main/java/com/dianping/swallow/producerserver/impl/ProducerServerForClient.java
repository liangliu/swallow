package com.dianping.swallow.producerserver.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.dpsf.api.ServiceRegistry;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServerForClient implements MQService {

   private static final Logger logger       = Logger.getLogger(ProducerServerForClient.class);
   private static final int    DEFAULT_PORT = 4000;
   private int                 port         = DEFAULT_PORT;

   @Autowired
   private MessageDAO          messageDAO;

   /**
    * 启动producerServerClient
    * 
    * @param port 供producer连接的端口
    * @throws Exception 连续绑定同一个端口抛出异常，pigeon初始化失败抛出异常
    */
   public void start() throws Exception {
      ServiceRegistry remoteService = null;
      try {
         remoteService = new ServiceRegistry(getPort());
         Map<String, Object> services = new HashMap<String, Object>();
         services.put("remoteService", this);
         remoteService.setServices(services);
         remoteService.init();
      } catch (Exception e) {
         logger.log(Level.ERROR, "[ProducerServer]:[Initialize remote service failed.]", e.getCause());
         throw e;
      }
   }

   /**
    * 保存swallowMessage到数据库
    */
   @Override
   public Packet sendMessage(Packet pkt) throws Exception {
      Packet pktRet = null;
      switch (pkt.getPacketType()) {
         case PRODUCER_GREET:
            System.out.println("got greet");
            try {
               //返回ProducerServer地址
               pktRet = new PktSwallowPACK(InetAddress.getLocalHost().toString());
            } catch (UnknownHostException uhe) {
               pktRet = new PktSwallowPACK(uhe.toString());
            }
            break;
         case OBJECT_MSG:
            String sha1 = SHAGenerater.generateSHA(((SwallowMessage) ((PktObjectMessage) pkt).getContent())
                  .getContent());
            pktRet = new PktSwallowPACK(sha1);

            //设置swallowMessage的sha-1
            ((SwallowMessage) ((PktObjectMessage) pkt).getContent()).setSha1(sha1);

            //将swallowMessage保存到mongodb
            try {
               messageDAO.saveMessage(((PktObjectMessage) pkt).getDestination().getName(),
                     (SwallowMessage) ((PktObjectMessage) pkt).getContent());
            } catch (Exception e) {
               logger.error("[ProducerServer]:[Message saved failed.]", e);
               throw e;
            }
            break;
         default:
            logger.log(Level.WARN, "[ProducerServer]:[Received unrecognized packet.]");
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
}
