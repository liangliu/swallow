package com.dianping.swallow.producer.impl;

import com.dianping.dpsf.exception.NetException;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

/**
 * Producer的异步模式消息处理类
 * 
 * @author tong.song
 */
public class HandlerSynchroMode {
   MQService remoteService;

   public HandlerSynchroMode(MQService remoteService) {
      this.remoteService = remoteService;
   }

   //对外接口
   public Packet doSendMsg(Packet pkt) throws ServerDaoException, RemoteServiceDownException{
      Packet pktRet = null;
      try {
         pktRet = remoteService.sendMessage(pkt);
      } catch (ServerDaoException e) {
         throw e;
      } catch(NetException e){
         throw new RemoteServiceDownException();
      }
      return pktRet;
   }
}
