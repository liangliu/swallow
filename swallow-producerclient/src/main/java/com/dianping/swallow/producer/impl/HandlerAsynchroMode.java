package com.dianping.swallow.producer.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dianping.filequeue.DefaultFileQueueImpl;
import com.dianping.filequeue.FileQueue;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.packet.Packet;

public class HandlerAsynchroMode {
   private Logger            logger = Logger.getLogger(Producer.class);
   private Producer          producer;
   private ExecutorService   senders;                                  //filequeue处理线程池
   private FileQueue<Packet> messageQueue;                             //filequeue

   //构造函数
   public HandlerAsynchroMode(Producer producer) {
      this.producer = producer;
      messageQueue = new DefaultFileQueueImpl<Packet>("filequeue.properties", producer.getDestination().getName());//filequeue
      senders = Executors.newFixedThreadPool(producer.getThreadPoolSize());
      this.start();
   }

   //对外的接口//异步处理只需将pkt放入filequeue即可，放入失败抛出异常
   public void doSendMsg(Packet pkt) throws FileQueueClosedException {
      messageQueue.add(pkt);
   }

   //启动处理线程
   private void start() {
      int idx;
      for (idx = 0; idx < producer.getThreadPoolSize(); idx++) {
         senders.execute(new TskGetAndSend());
      }
   }

   //从filequeue队列获取并发送Message的runnable
   private class TskGetAndSend implements Runnable {
      @Override
      public void run() {
         while (true) {
            //如果filequeue无元素则阻塞，否则发送
            try {
               producer.getRemoteService().sendMessage(messageQueue.get());
            } catch (Exception e) {
               logger.log(Level.ERROR, "[SendMessage]:[Message sent failed.]", e.getCause());
               continue;
            }
         }
      }
   }
}
