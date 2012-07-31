package com.dianping.swallow.producer.impl.internal;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.internal.threadfactory.DefaultPullStrategy;
import com.dianping.swallow.common.message.Destination;
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
   private final Destination         destination;
   private ProducerSwallowService    remoteService;
   private static final int          DELAY_BASE_MULTI = 5; //超时策略倍数

   public HandlerSynchroMode(ProducerImpl producer) {
      this.sendTimes = producer.getProducerConfig().getSyncRetryTimes() + 1;//初始值等于用户要求的retryTimes+1，这样可以保证至少执行一次
      this.delayBase = producer.getPunishTimeout();
      this.remoteService = producer.getRemoteService();
      this.destination = producer.getDestination();
      defaultPullStrategy = new DefaultPullStrategy(delayBase, DELAY_BASE_MULTI * delayBase);
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

      Transaction t = Cat.getProducer().newTransaction("MessageProduced", destination.getName());
      Event event = Cat.getProducer().newEvent("Message", "Payload");
      if (pktRet != null) {
         event.addData(((PktSwallowPACK) pktRet).getShaInfo());
         event.setStatus(Message.SUCCESS);
         t.setStatus(Message.SUCCESS);
      } else {
         event.complete();
         t.complete();
      }
      
      return pktRet;
   }
}
