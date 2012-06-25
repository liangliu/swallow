package com.dianping.swallow.producerserver.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dianping.dpsf.api.ServiceRegistry;
import com.dianping.swallow.common.dao.impl.mongodb.MessageDAOImpl;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServer implements MQService {

   private static final Logger         logger               = Logger.getLogger(ProducerServer.class);
   private static final int            defaultPortForClient = 4000;
   private static final int            defaultPortForText   = 8000;
   private static final MessageDAOImpl messageDAO           = new MessageDAOImpl();
   private static ProducerServer       instance;

   /**
    * 构造函数
    * 
    * @param portForText
    */
   private ProducerServer(int portForText) {
      //启动文本流处理
      new ProducerServerText(this).start(portForText);
   }

   /**
    * 使用默认端口启动swallowServer
    * 
    * @throws Exception
    */
   public static void start() throws Exception {
      startAt(defaultPortForClient, defaultPortForText);
   }

   /**
    * 启动swallowServer
    * 
    * @param portForClient 供producer连接的端口
    * @param portForText 供文本流使用的端口
    * @throws Exception 连续绑定同一个端口抛出异常，pigeon初始化失败抛出异常
    */
   public static void startAt(int portForClient, int portForText) throws Exception {
      if (instance != null) {
         logger.log(Level.ERROR, "Already exist a ProducerServer instance.");
         throw new Exception("Exist another ProducerServer instance, please turn off it before start a new one.");
      }

      ServiceRegistry remoteService = null;
      instance = new ProducerServer(portForText);
      try {
         remoteService = new ServiceRegistry(portForClient);
         Map<String, Object> services = new HashMap<String, Object>();
         services.put("remoteService", instance);
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
               logger.log(Level.ERROR, "[ProducerServer]:[Message saved failed.]");
               throw e;
            }
            break;
         default:
            logger.log(Level.WARN, "[ProducerServer]:[Received unrecognized packet.]");
            break;
      }
      return pktRet;
   }

   public static void main(String[] args) throws Exception {
      ProducerServer.start();
   }
}
