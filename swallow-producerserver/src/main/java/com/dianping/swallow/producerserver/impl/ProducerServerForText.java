package com.dianping.swallow.producerserver.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.dianping.swallow.common.dao.MessageDAO;

public class ProducerServerForText {
   private static final int    DEFAULT_PORT = 8000;
   private int                 port         = DEFAULT_PORT;
   private static final Logger logger       = Logger.getLogger(ProducerServerForText.class);
   private MessageDAO          messageDAO;
   private long receivedMessageNum = 0;

   public void start() {
      ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new ProducerServerTextPipelineFactory(messageDAO));
      bootstrap.bind(new InetSocketAddress(getPort()));
      logger.info("[Initialize netty sucessfully, Producer service for text is ready.]");
   }

   public int getPort() {
      return port;
   }

   public void setPort(int port) {
      this.port = port;
   }

   public void setMessageDAO(MessageDAO messageDAO) {
      this.messageDAO = messageDAO;
   }

}
