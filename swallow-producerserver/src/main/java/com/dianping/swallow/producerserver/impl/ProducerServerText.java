package com.dianping.swallow.producerserver.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.swallow.common.dao.MessageDAO;

public class ProducerServerText {
   private static final int DEFAULT_PORT = 8000;
   private int port = DEFAULT_PORT;
   
   @Autowired
   private MessageDAO messageDAO;

   public void start() {
      ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new ProducerServerTextPipelineFactory(messageDAO));
      bootstrap.bind(new InetSocketAddress(getPort()));
   }

   public int getPort() {
      return port;
   }

   public void setPort(int port) {
      this.port = port;
   }
}
