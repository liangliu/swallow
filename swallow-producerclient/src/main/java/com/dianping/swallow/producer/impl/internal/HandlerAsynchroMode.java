package com.dianping.swallow.producer.impl.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.dpsf.exception.NetException;
import com.dianping.filequeue.DefaultFileQueueConfig.FileQueueConfigHolder;
import com.dianping.filequeue.DefaultFileQueueImpl;
import com.dianping.filequeue.FileQueue;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.internal.producer.MQService;
import com.dianping.swallow.common.internal.threadfactory.DefaultPullStrategy;
import com.dianping.swallow.common.internal.threadfactory.MQThreadFactory;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

/**
 * Producer的异步模式消息处理类
 * 
 * @author tong.song
 */
public class HandlerAsynchroMode {
   private static final Logger          logger                 = LoggerFactory.getLogger(ProducerImpl.class);
   private static final MQThreadFactory threadFactory          = new MQThreadFactory();

   private static final int             DEFAULT_FILEQUEUE_SIZE = 512 * 1024 * 1024;                            //默认的filequeue切片大小，512MB
   private static final int             DELAY_BASE             = ProducerFactoryImpl.getRemoteServiceTimeout();

   private final ProducerImpl           producer;
   private final FileQueue<Packet>      messageQueue;                                                          //filequeue

   //构造函数
   public HandlerAsynchroMode(ProducerImpl producer) {
      this.producer = producer;

      FileQueueConfigHolder fileQueueConfig = new FileQueueConfigHolder();
      fileQueueConfig.setMaxDataFileSize(DEFAULT_FILEQUEUE_SIZE);
      messageQueue = new DefaultFileQueueImpl<Packet>(fileQueueConfig, producer.getDestination().getName(),
            producer.isContinueSend());

      this.start();
   }

   //对外的接口//异步处理只需将pkt放入filequeue即可，放入失败抛出异常
   public void doSendMsg(Packet pkt) throws FileQueueClosedException {
      messageQueue.add(pkt);
   }

   //启动处理线程
   private void start() {
      int idx;
      int threadPoolSize = producer.getThreadPoolSize();
      for (idx = 0; idx < threadPoolSize; idx++) {
         threadFactory.newThread(new TskGetAndSend(), "AsyncProducer_" + idx).start();
      }
   }

   //从filequeue队列获取并发送Message
   private class TskGetAndSend implements Runnable {

      private final int sendTimes = producer.getRetryTimes() + 1;
      private int       leftRetryTimes    = sendTimes;
      private Packet    message           = null;
      private MQService remoteService     = producer.getRemoteService();

      @Override
      public void run() {
         //异步模式下，每个线程单独有一个延时策略，以保证不同的线程不会互相冲突
         DefaultPullStrategy defaultPullStrategy = new DefaultPullStrategy(DELAY_BASE, 5 * DELAY_BASE);

         while (true) {
            //从filequeue获取message，如果filequeue无元素则阻塞            
            message = messageQueue.get();
            //发送message，重试次数从Producer获取
            for (leftRetryTimes = sendTimes; leftRetryTimes > 0;) {
               try {
                  leftRetryTimes--;
                  remoteService.sendMessage(message);
               } catch (ServerDaoException e) {
                  //如果剩余重试次数>0，超时重试
                  if (leftRetryTimes > 0) {
                     try {
                        defaultPullStrategy.fail(true);
                     } catch (InterruptedException ie) {
                        //睡眠失败则不睡眠直接发送
                     }
                     //发送失败，重发
                     continue;
                  }
                  logger.error("[AsyncHandler]:[Message sent failed.][Reason=DAO]", e);
               } catch (NetException e) {
                  if (leftRetryTimes > 0) {
                     try {
                        defaultPullStrategy.fail(true);
                     } catch (InterruptedException ie) {
                        //睡眠失败则不睡眠直接发送
                     }
                     //发送失败，重发
                     continue;
                  }
                  logger.error("[AsyncHandler]:[Message sent failed.][Reason=Network]", e);
               } catch (Exception e) {
                  //捕获到未知异常，记录
                  logger.error("[AsyncHandler]:[Unknow Exception]", e);
                  try {
                     defaultPullStrategy.fail(true);
                  } catch (InterruptedException ie) {
                     //睡眠失败则不睡眠直接发送
                  }
                  continue;
               }
               //如果发送成功则跳出循环
               break;
            }
            //跳出循环，说明消息发送成功break，或重试次数消耗完，此时重置延时
            defaultPullStrategy.succeess();
         }
      }
   }
}
