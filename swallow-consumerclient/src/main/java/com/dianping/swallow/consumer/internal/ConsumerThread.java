package com.dianping.swallow.consumer.internal;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerThread的作用是，它会不断的保持与ConsumerServer的连接(一个channel关闭后继续建立新的channel)
 * 
 * @author wukezhu
 */
public class ConsumerThread implements Runnable {

   private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

   private ClientBootstrap     bootstrap;

   private InetSocketAddress   remoteAddress;

   private long                interval;

   public ClientBootstrap getBootstrap() {
      return bootstrap;
   }

   public void setBootstrap(ClientBootstrap bootstrap) {
      this.bootstrap = bootstrap;
   }

   public void setRemoteAddress(InetSocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
   }

   public void setInterval(long interval) {
      this.interval = interval;
   }

   @Override
   public void run() {
      while (!Thread.currentThread().isInterrupted()) {
         synchronized (bootstrap) {
            try {
               ChannelFuture future = bootstrap.connect(remoteAddress);
               LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + ")-connected to " + remoteAddress);
               future.getChannel().getCloseFuture().await();//等待channel关闭，否则一直阻塞！
               LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + ")-closed from " + remoteAddress);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
               LOG.error(e.getMessage(), e);
            }
         }
         try {
            Thread.sleep(interval);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + " ,remoteAddress=" + remoteAddress
            + ") done.");
   }
}
