package com.dianping.swallow.producerserver.impl;

import java.util.HashMap;
import java.util.Map;

import com.dianping.dpsf.api.ServiceRegistry;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServer implements MQService {

   private static ProducerServer instance;
   private TopicDAOImpl          topicDAO = new TopicDAOImpl();

   private ProducerServer(int portForText) {
      new ProducerServerText(this).start(portForText);
   }

   public static void startAt(int portForClient, int portForText) throws Exception {
      if (instance != null){
         throw new Exception("Exist another ProducerServer instance, please turn off it before start a new one.");
      }
      
      instance = new ProducerServer(portForText);
      ServiceRegistry remoteService = new ServiceRegistry(portForClient);
      Map<String, Object> services = new HashMap<String, Object>();
      services.put("remoteService", instance);
      remoteService.setServices(services);
      remoteService.init();
   }

   @Override
   public void sendMessageWithoutReturn(Packet pkt) {
      sendMessage(pkt);
   }

   @Override
   public Packet sendMessage(Packet pkt) {
      Packet pktRet = null;
      switch (pkt.getPacketType()) {
         case PRODUCER_GREET:
            pktRet = new PktSwallowPACK(SHAGenerater.generateSHA(((PktProducerGreet) pkt).getProducerVersion()));
            break;
         case OBJECT_MSG:
            String sha1 = SHAGenerater.generateSHA(((SwallowMessage) ((PktObjectMessage) pkt).getContent())
                  .getContent());
            pktRet = new PktSwallowPACK(sha1);
            ((SwallowMessage) ((PktObjectMessage) pkt).getContent()).setSha1(sha1);

            //TODO 发生异常如何处理？
            topicDAO.saveMessage(((PktObjectMessage) pkt).getDestination().getName(),
                  (SwallowMessage) ((PktObjectMessage) pkt).getContent());
            break;
         default:
            //TODO log it
            break;
      }
      return pktRet;
   }

   public static void main(String[] args) throws Exception {
         ProducerServer.startAt(4000, 8000);
   }
}
