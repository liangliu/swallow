package com.dianping.swallow.producerserver.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.internal.dao.MessageDAO;

public class ProducerServerForText {
   private static final int    DEFAULT_PORT = 8000;
   private int                 port         = DEFAULT_PORT;
   private static final Logger logger       = Logger.getLogger(ProducerServerForText.class);
   private MessageDAO          messageDAO;

   public ProducerServerForText() {
      //Hawk监控
      HawkJMXUtil.registerMBean("ProducerServerForText", new HawkMBean());
   }

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

   /**
    * 用于Hawk监控
    */
   public class HawkMBean {
      public int getPort() {
         return port;
      }
   }

}
