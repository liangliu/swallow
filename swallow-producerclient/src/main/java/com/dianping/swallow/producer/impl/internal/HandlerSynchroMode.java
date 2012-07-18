package com.dianping.swallow.producer.impl.internal;

import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.internal.threadfactory.DefaultPullStrategy;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.ProducerHandler;

/**
 * Producer的同步模式消息处理类
 * 
 * @author tong.song
 */
public class HandlerSynchroMode implements ProducerHandler {
   private final int                 sendTimes;
   private final int                 delayBase;
   private final DefaultPullStrategy defaultPullStrategy;
   private ProducerSwallowService    remoteService;

   public HandlerSynchroMode(ProducerImpl producer) {
      this.sendTimes = producer.getProducerConfig().getRetryTimes() + 1;//初始值等于用户要求的retryTimes+1，这样可以保证至少执行一次
      this.delayBase = producer.getRemoteServiceTimeout();
      this.remoteService = producer.getRemoteService();
      defaultPullStrategy = new DefaultPullStrategy(delayBase, 5 * delayBase);
   }

   //对外接口
   @Override
   public Packet doSendMsg(Packet pkt) throws SendFailedException {
      defaultPullStrategy.succeess();
      Packet pktRet = null;
      for (int leftRetryTimes = sendTimes; leftRetryTimes > 0;) {
         leftRetryTimes--;
         try {
            pktRet = remoteService.sendMessage(pkt);
         } catch (Exception e) {
            //如果剩余重试次数>0，继续重试
            if (leftRetryTimes > 0) {
               try {
                  defaultPullStrategy.fail(true);
               } catch (InterruptedException ie) {
                  return null;
               }
               continue;
            } else {
               //重置超时
               throw new SendFailedException("Message sent failed", e);
            }
         }
         break;
      }
      return pktRet;
   }
}
