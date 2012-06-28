package com.dianping.swallow.producer.impl;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

public class HandlerSynchroMode {
   private ProducerImpl producer;

   public HandlerSynchroMode(ProducerImpl producer) {
      this.producer = producer;
   }

   //对外接口
   public Packet doSendMsg(Packet pkt) throws ServerDaoException{
      Packet pktRet = null;
      pktRet = producer.getRemoteService().sendMessage(pkt);
      return pktRet;
   }
}
