package com.dianping.swallow.consumer.internal;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      while (true) {
         synchronized (bootstrap) {
            try {
               ChannelFuture future = bootstrap.connect(remoteAddress);
               future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞！
            } catch (RuntimeException e) {
               LOG.error("Unexpected exception", e);
            }
         }
         try {
            Thread.sleep(interval);
         } catch (InterruptedException e) {
            LOG.error("ConSlaveThread InterruptedException", e);
         }
      }
   }
}
