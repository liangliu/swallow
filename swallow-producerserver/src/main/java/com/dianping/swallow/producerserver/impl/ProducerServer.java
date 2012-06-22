package com.dianping.swallow.producerserver.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.dianping.dpsf.api.ServiceRegistry;
import com.dianping.swallow.common.dao.impl.mongodb.MessageDAOImpl;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.producerserver.util.SHAGenerater;

public class ProducerServer implements MQService {

   private static ProducerServer instance;
   private MessageDAOImpl        messageDAO = new MessageDAOImpl();

   private ProducerServer(int portForText) {
      new ProducerServerText(this).start(portForText);
   }

   public static void startAt(int portForClient, int portForText) throws Exception {
      if (instance != null) {
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
   public Packet sendMessage(Packet pkt) {
      Packet pktRet = null;
      switch (pkt.getPacketType()) {
         case PRODUCER_GREET:
            try {
               pktRet = new PktSwallowPACK(InetAddress.getLocalHost().toString());
            } catch (UnknownHostException uhe) {
               pktRet = new PktSwallowPACK(uhe.toString());
            }
            break;
         case OBJECT_MSG:
            String sha1 = SHAGenerater.generateSHA(((SwallowMessage) ((PktObjectMessage) pkt).getContent())
                  .getContent());
            pktRet = new PktSwallowPACK(sha1);
            ((SwallowMessage) ((PktObjectMessage) pkt).getContent()).setSha1(sha1);

            //TODO 发生异常如何处理？
            try {
               messageDAO.saveMessage(((PktObjectMessage) pkt).getDestination().getName(),
                     (SwallowMessage) ((PktObjectMessage) pkt).getContent());
            } catch (Exception e) {
               System.out.println("Message saved failed.");
            }
            break;
         default:
            //TODO log it
            System.out.println("Received unrecognized packet.");
            break;
      }
      return pktRet;
   }

   public static void main(String[] args) throws Exception {
      ProducerServer.startAt(4000, 8000);
   }
}
