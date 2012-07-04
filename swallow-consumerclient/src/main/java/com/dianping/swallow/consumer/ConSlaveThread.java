package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConSlaveThread implements Runnable {

   private static final Logger LOG             = LoggerFactory.getLogger(ConSlaveThread.class);

   private ClientBootstrap     bootstrap;

   private InetSocketAddress   slaveAddress;

   private long                waitTime        = 1000L;

   private long                connectInterval = 1000L;

   public ClientBootstrap getBootstrap() {
      return bootstrap;
   }

   public void setBootstrap(ClientBootstrap bootstrap) {
      this.bootstrap = bootstrap;
   }

   public void setSlaveAddress(InetSocketAddress slaveAddress) {
      this.slaveAddress = slaveAddress;
   }

   @Override
   public void run() {
      try {
         Thread.sleep(waitTime);
         while (true) {
            synchronized (bootstrap) {
               ChannelFuture future = bootstrap.connect(slaveAddress);
               future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞！	  
            }
            Thread.sleep(connectInterval);
         }
      } catch (InterruptedException e) {
         LOG.error("thread InterruptedException", e);
      }
   }
}
