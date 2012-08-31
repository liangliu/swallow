package com.dianping.swallow.producerserver.impl;

import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.internal.dao.MessageDAO;

public class ProducerServerForText {
   private static final int    DEFAULT_PORT = 8000;
   private int                 port         = DEFAULT_PORT;
   private static final Logger LOGGER       = LoggerFactory.getLogger(ProducerServerForText.class);
   private MessageDAO          messageDAO;

   public ProducerServerForText() {
      //Hawk监控
      HawkJMXUtil.unregisterMBean("ProducerServerForText");
      HawkJMXUtil.registerMBean("ProducerServerForText", new HawkMBean(this));
   }

   public void start() {
      ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new ProducerServerTextPipelineFactory(messageDAO));
      bootstrap.bind(new InetSocketAddress(getPort()));
      LOGGER.info("[Initialize netty sucessfully, Producer service for text is ready.]");
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
   public static class HawkMBean {

      private final WeakReference<ProducerServerForText> producerServerForText;

      private HawkMBean(ProducerServerForText producerServerForText) {
         this.producerServerForText = new WeakReference<ProducerServerForText>(producerServerForText);
      }

      public int getPort() {
         return (producerServerForText.get() != null) ? producerServerForText.get().port : null;
      }
   }

}
