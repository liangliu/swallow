package com.dianping.swallow.consumerserver;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.consumerserver.worker.ConsumerWorkerImpl;

public class HandleACKThread implements Runnable, Closeable {

   private static Logger      LOG = LoggerFactory.getLogger(HandleACKThread.class);

   private ConsumerWorkerImpl consumerInformation;

   public void setConsumerInformation(ConsumerWorkerImpl consumerInformation) {
      this.consumerInformation = consumerInformation;
   }

   private volatile boolean isLive = true;

   @Override
   public void close() {
      LOG.info("receive close command");
      LOG.info("closing");
      isLive = false;
   }

   @Override
   public void run() {
      ArrayBlockingQueue<Runnable> ackWorker = consumerInformation.getAckWorker();
      while (isLive) {
         Runnable worker = null;
         try {
            while (true) {
               worker = ackWorker.poll(1000, TimeUnit.MILLISECONDS);// TODO 
               if (worker != null) {
                  worker.run();
               } else {
                  break;
               }
            }
         } catch (InterruptedException e) {
            LOG.error("unexpected interrupt", e);
         }
         synchronized (consumerInformation.getConnectedChannels()) {
            if (consumerInformation.getConnectedChannels().isEmpty()) {
               consumerInformation.setHandleACKThreadExist(false);
               isLive = false;
            }
         }

      }
      LOG.info("closed");
   }

}
