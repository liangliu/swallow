package com.dianping.swallow.producer.impl;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dianping.filequeue.DefaultFileQueueConfig.FileQueueConfigHolder;
import com.dianping.filequeue.DefaultFileQueueImpl;
import com.dianping.filequeue.FileQueue;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.threadfactory.MQThreadFactory;

public class HandlerAsynchroMode {
   private Logger            logger        = Logger.getLogger(ProducerImpl.class);
   private ProducerImpl      producer;
   private FileQueue<Packet> messageQueue;                                        //filequeue

   private MQThreadFactory   threadFactory = new MQThreadFactory();

   //构造函数
   public HandlerAsynchroMode(ProducerImpl producer) {
      FileQueueConfigHolder fileQueueConfig = new FileQueueConfigHolder();
      this.producer = producer;
      fileQueueConfig.setMaxDataFileSize(512 * 1024 * 1024);//默认512M
      messageQueue = new DefaultFileQueueImpl<Packet>(fileQueueConfig, producer.getDestination().getName(), producer.isContinueSend());
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

   //从filequeue队列获取并发送Message的runnable
   private class TskGetAndSend implements Runnable {

      private final int defaultRetryTimes = producer.getRetryTimes();
      private int       leftRetryTimes    = defaultRetryTimes;
      private Packet    message           = null;
      private MQService remoteService     = producer.getRemoteService();

      @Override
      public void run() {
         while (true) {
            //从filequeue获取message，如果filequeue无元素则阻塞            
            message = messageQueue.get();
            //发送message，重试次数从Producer获取
            for (leftRetryTimes = defaultRetryTimes; leftRetryTimes > 0; leftRetryTimes--) {
               try {
                  remoteService.sendMessage(message);
               } catch (ServerDaoException e) {
                  logger.log(Level.ERROR, "[SendMessage]:[Message sent failed.]", e.getCause());
                  //发送失败，重发
                  continue;
               }
               //如果发送成功则跳出循环//TODO 需要日志记录消息重发的次数吗？
               break;
            }
         }
      }
   }
}
