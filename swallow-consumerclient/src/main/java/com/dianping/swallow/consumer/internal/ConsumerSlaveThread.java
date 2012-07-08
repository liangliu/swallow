package com.dianping.swallow.consumer.internal;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.consumer.internal.config.ConfigManager;


public class ConsumerSlaveThread implements Runnable {

   private static final Logger LOG             = LoggerFactory.getLogger(ConsumerSlaveThread.class);

   private ClientBootstrap     bootstrap;

   private InetSocketAddress   slaveAddress;
   
   private ConfigManager configManager;

   public ClientBootstrap getBootstrap() {
      return bootstrap;
   }

   public void setConfigManager(ConfigManager configManager) {
      this.configManager = configManager;
   }

   public void setBootstrap(ClientBootstrap bootstrap) {
      this.bootstrap = bootstrap;
   }

   public void setSlaveAddress(InetSocketAddress slaveAddress) {
      this.slaveAddress = slaveAddress;
   }

   @Override
   public void run() {
      while (true) {
         synchronized (bootstrap) {
            ChannelFuture future = bootstrap.connect(slaveAddress);
            future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞！              
         }
         try {
            Thread.sleep(configManager.getConnectSlaveInterval());
         } catch (InterruptedException e) {
            LOG.error("ConSlaveThread InterruptedException", e);
         }
      }
   }
}
