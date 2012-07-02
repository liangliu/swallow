package com.dianping.swallow.consumerserver;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.consumerserver.worker.ConsumerWorkerImpl;

public class GetMessageThread implements Runnable, Closeable {

   private static Logger      LOG    = LoggerFactory.getLogger(GetMessageThread.class);

   private volatile boolean   isLive = true;
   private ConsumerWorkerImpl consumerInformation;

   public void setConsumerInformation(ConsumerWorkerImpl consumerInformation) {
      this.consumerInformation = consumerInformation;
   }

   @Override
   public void close() {
      LOG.info("receive close command");
      LOG.info("closing");
      isLive = false;
   }

   @Override
   public void run() {
      while (isLive) {
         consumerInformation.sendMessageByPollFreeChannelQueue();
         synchronized (consumerInformation.getConnectedChannels()) {
            if (consumerInformation.getConnectedChannels().isEmpty()) {
               consumerInformation.setGetMessageThreadExist(false);
               isLive = false;
            }
         }

      }
      LOG.info("closed");
   }

}
