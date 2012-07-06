package com.dianping.swallow.producer.impl;

import com.dianping.dpsf.exception.NetException;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.threadfactory.DefaultPullStrategy;

/**
 * Producer的同步模式消息处理类
 * 
 * @author tong.song
 */
public class HandlerSynchroMode {
   private MQService           remoteService;
   private int                 retryTimes;
   private int                 delayBase           = ProducerFactoryImpl.getRemoteServiceTimeout();
   private DefaultPullStrategy defaultPullStrategy = new DefaultPullStrategy(delayBase, 10 * delayBase);

   public HandlerSynchroMode(ProducerImpl producer) {
      this.remoteService = producer.getRemoteService();
      this.retryTimes = producer.getRetryTimes() +1;//初始值等于用户要求的retryTimes+1，这样可以保证至少执行一次
   }

   //对外接口
   public Packet doSendMsg(Packet pkt) throws ServerDaoException, RemoteServiceDownException {
      Packet pktRet = null;
      int leftRetryTimes;
      for (leftRetryTimes = retryTimes; leftRetryTimes > 0;) {
         try {
            leftRetryTimes--;
            pktRet = remoteService.sendMessage(pkt);
         } catch (ServerDaoException e) {
            //如果剩余重试次数>0，继续重试
            if (leftRetryTimes > 0) {
               try {
                  defaultPullStrategy.fail(true);
               } catch (InterruptedException ie) {
                  //睡眠失败则不睡眠直接发送
               }
               continue;
            } else {
               throw e;
            }
         } catch (NetException e) {
            //如果剩余重试次数>0，继续重试
            if (leftRetryTimes > 0) {
               try {
                  defaultPullStrategy.fail(true);
               } catch (InterruptedException ie) {
                  //睡眠失败则不睡眠直接发送
               }
               continue;
            } else {
               throw new RemoteServiceDownException();
            }
         } catch (Exception e) {
            try {
               defaultPullStrategy.fail(true);
            } catch (InterruptedException ie) {
               //睡眠失败则不睡眠直接发送
            }
            e.printStackTrace();
            continue;
         }
         defaultPullStrategy.succeess();
         break;
      }

      return pktRet;
   }
}
