package com.dianping.swallow.consumer.internal;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerThread的作用是，它会不断的保持与ConsumerServer的连接(一个channel关闭后继续建立新的channel)<br>
 * 使用时，一个Consumer拥有master和slave2个线程，2个线程不断交替尝试连接master和slave服务器，直到其中一个连接成功。<br>
 * 当master和slave线程其中一个连接成功后，另外一个线程将阻塞<br>
 * 已经连接成功的线程，当连接被关闭后，会睡眠一会，然后继续进入2个线程交替尝试的场景。<br>
 * 
 * @author wukezhu
 */
public class ConsumerThread extends Thread {

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
            if (!Thread.currentThread().isInterrupted()) {
               try {
                  LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + ")-try connecting to "
                        + remoteAddress);
                  ChannelFuture future = bootstrap.connect(remoteAddress);
                  future.awaitUninterruptibly();
                  if (future.getChannel().isConnected()) {
                     LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + ")-connected to "
                           + remoteAddress);
                     future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞！
                     LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + ")-closed from "
                           + remoteAddress);
                  }
               } catch (RuntimeException e) {
                  LOG.error(e.getMessage(), e);
               }
            }
         }
         try {
            Thread.sleep(interval);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      LOG.info("ConsumerThread(name=" + Thread.currentThread().getName() + ",remoteAddress=" + remoteAddress
            + ") done.");
   }
}
