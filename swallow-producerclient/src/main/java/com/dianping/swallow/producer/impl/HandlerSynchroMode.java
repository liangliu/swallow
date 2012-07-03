package com.dianping.swallow.producer.impl;

import com.dianping.dpsf.exception.NetException;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

/**
 * Producer的同步模式消息处理类
 * 
 * @author tong.song
 */
public class HandlerSynchroMode {
   private MQService    remoteService;
   private int          retryTimes;

   public HandlerSynchroMode(ProducerImpl producer) {
      this.remoteService = producer.getRemoteService();
      this.retryTimes = producer.getRetryTimes();
   }

   //对外接口
   public Packet doSendMsg(Packet pkt) throws ServerDaoException, RemoteServiceDownException {
      Packet pktRet = null;
      int leftRetryTimes;
      for (leftRetryTimes = retryTimes; leftRetryTimes > 0; ) {
         try {
            leftRetryTimes--;
            pktRet = remoteService.sendMessage(pkt);
         } catch (ServerDaoException e) {
            //如果剩余重试次数>1，继续重试
            if (leftRetryTimes > 0)
               continue;
            throw e;
         } catch (NetException e) {
            //如果剩余重试次数>1，继续重试
            if (leftRetryTimes > 0)
               continue;
            throw new RemoteServiceDownException();
         } catch(Exception e){
            continue;
         }
         break;
      }

      return pktRet;
   }
}
