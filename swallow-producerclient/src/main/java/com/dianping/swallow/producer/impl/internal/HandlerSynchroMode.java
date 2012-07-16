package com.dianping.swallow.producer.impl.internal;

import com.dianping.dpsf.exception.NetException;
import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.internal.threadfactory.DefaultPullStrategy;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.producer.ProducerHandler;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

/**
 * Producer的同步模式消息处理类
 * 
 * @author tong.song
 */
public class HandlerSynchroMode implements ProducerHandler {
   private ProducerSwallowService remoteService;
   private int                    sendTimes;
   private int                    delayBase           = ProducerFactoryImpl.getRemoteServiceTimeout();
   private DefaultPullStrategy    defaultPullStrategy = new DefaultPullStrategy(delayBase, 5 * delayBase);

   public HandlerSynchroMode(ProducerImpl producer) {
      this.remoteService = producer.getRemoteService();
      this.sendTimes = producer.getProducerConfig().getRetryTimes() + 1;//初始值等于用户要求的retryTimes+1，这样可以保证至少执行一次
   }

   //对外接口
   @Override
   public Packet doSendMsg(Packet pkt) throws SendFailedException {
      Packet pktRet = null;
      int leftRetryTimes;
      for (leftRetryTimes = sendTimes; leftRetryTimes > 0;) {
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
               //重置超时
               defaultPullStrategy.succeess();
               throw new SendFailedException("Can not save message to DB.", e);
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
               //重置超时
               defaultPullStrategy.succeess();
               throw new SendFailedException("Can not send message to swallow.", e);
            }
         } catch (Exception e) {
            e.printStackTrace();
            try {
               defaultPullStrategy.fail(true);
            } catch (InterruptedException ie) {
               //睡眠失败则不睡眠直接发送
            }
            continue;
         }
         break;
      }
      //能跳出循环，重试次数消耗完OR消息发送成功，循环break，此时重置超时时间
      defaultPullStrategy.succeess();
      return pktRet;
   }
}
