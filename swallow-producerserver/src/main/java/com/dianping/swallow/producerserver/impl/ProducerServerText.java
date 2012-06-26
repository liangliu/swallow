package com.dianping.swallow.producerserver.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.dianping.swallow.common.dao.impl.mongodb.MessageDAOImpl;

public class ProducerServerText {
   private MessageDAOImpl messageDAO;

   public ProducerServerText(MessageDAOImpl messageDAO) {
      this.messageDAO = messageDAO;
   }

   public void start(int portForText) {
      ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new ProducerServerTextPipelineFactory(messageDAO));
      bootstrap.bind(new InetSocketAddress(portForText));
   }
}
