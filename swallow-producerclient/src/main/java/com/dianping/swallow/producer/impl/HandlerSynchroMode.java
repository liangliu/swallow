package com.dianping.swallow.producer.impl;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

public class HandlerSynchroMode {
   MQService remoteService;
   

   public HandlerSynchroMode(MQService remoteService) {
      this.remoteService = remoteService;
   }

   //对外接口
   public Packet doSendMsg(Packet pkt) throws ServerDaoException{
      Packet pktRet = null;
      pktRet = remoteService.sendMessage(pkt);
      return pktRet;
   }
}
