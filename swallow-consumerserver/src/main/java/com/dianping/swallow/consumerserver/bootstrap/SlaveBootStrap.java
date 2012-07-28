package com.dianping.swallow.consumerserver.bootstrap;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.dianping.cat.Cat;
import com.dianping.swallow.common.internal.codec.JsonDecoder;
import com.dianping.swallow.common.internal.codec.JsonEncoder;
import com.dianping.swallow.common.internal.monitor.CloseMonitor;
import com.dianping.swallow.common.internal.monitor.CloseMonitor.CloseHook;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.consumerserver.Heartbeater;
import com.dianping.swallow.consumerserver.config.ConfigManager;
import com.dianping.swallow.consumerserver.netty.MessageServerHandler;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class SlaveBootStrap {

   private static final Logger     LOG       = LoggerFactory.getLogger(SlaveBootStrap.class);

   private static boolean          isSlave   = true;
   private static ServerBootstrap  bootstrap = null;
   private static volatile boolean closed    = false;

   private SlaveBootStrap() {
   }

   private static void closeNettyRelatedResource() {
      try {
         LOG.info("MessageServerHandler.getChannelGroup().unbind()-started");
         MessageServerHandler.getChannelGroup().unbind().await();
         LOG.info("MessageServerHandler.getChannelGroup().unbind()-finished");

         LOG.info("MessageServerHandler.getChannelGroup().close()-started");
         MessageServerHandler.getChannelGroup().close().await();
         LOG.info("MessageServerHandler.getChannelGroup().close()-finished");

         LOG.info("MessageServerHandler.getChannelGroup().clear()-started");
         MessageServerHandler.getChannelGroup().clear();
         LOG.info("MessageServerHandler.getChannelGroup().unbind()-finished");

         LOG.info("bootstrap.releaseExternalResources()-started");
         bootstrap.releaseExternalResources();
         LOG.info("bootstrap.releaseExternalResources()-finished");
      } catch (InterruptedException e) {
         LOG.error("Interrupted when closeNettyRelatedResource()", e);
         Thread.currentThread().interrupt();
      }
   }

   /**
    * 启动Slave
    */
   public static void main(String[] args) {
      //启动Cat
      Cat.initialize(new File("/data/appdatas/cat/client.xml"));

      ApplicationContext ctx = new ClassPathXmlApplicationContext(
            new String[] { "applicationContext-consumerserver.xml" });
      ConfigManager configManager = ConfigManager.getInstance();

      final ConsumerWorkerManager consumerWorkerManager = ctx.getBean(ConsumerWorkerManager.class);

      Heartbeater heartbeater = ctx.getBean(Heartbeater.class);

      CloseMonitor closeMonitor = new CloseMonitor();
      int port = Integer.parseInt(System.getProperty("closeMonitorPort", "17556"));
      closeMonitor.start(port, new CloseHook() {

         @Override
         public void onClose() {
            closed = true;
            LOG.info("consumerWorkerManager.close()-started");
            consumerWorkerManager.close();
            LOG.info("consumerWorkerManager.close()-finished");
            closeNettyRelatedResource();
            Thread.currentThread().interrupt();
         }

      });

      LOG.info("slave starting, master ip: " + configManager.getMasterIp());
      consumerWorkerManager.init(isSlave);

      while (!closed) {

         try {
            heartbeater.waitUntilMasterDown(configManager.getMasterIp(), configManager.getHeartbeatCheckInterval(),
                  configManager.getHeartbeatMaxStopTime());
         } catch (InterruptedException e) {
            LOG.info("slave interruptted, will stop", e);
            break;
         }
         // Configure the server.
         bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
               Executors.newCachedThreadPool()));

         bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
               MessageServerHandler handler = new MessageServerHandler(consumerWorkerManager);
               ChannelPipeline pipeline = Channels.pipeline();
               pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
               pipeline.addLast("jsonDecoder", new JsonDecoder(PktConsumerMessage.class));
               pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
               pipeline.addLast("jsonEncoder", new JsonEncoder(PktMessage.class));
               pipeline.addLast("handler", handler);
               return pipeline;
            }
         });

         // Bind and start to accept incoming connections.
         bootstrap.bind(new InetSocketAddress(consumerWorkerManager.getConfigManager().getSlavePort()));
         LOG.info("Server started on port " + consumerWorkerManager.getConfigManager().getSlavePort());

         try {
            heartbeater.waitUntilMasterUp(configManager.getMasterIp(), configManager.getHeartbeatCheckInterval(),
                  configManager.getHeartbeatMaxStopTime());
         } catch (InterruptedException e) {
            LOG.info("slave interruptted, will stop", e);
            break;
         }
         LOG.info("consumerWorkerManager.close()-started");
         consumerWorkerManager.close();
         LOG.info("consumerWorkerManager.close()-finished");
         closeNettyRelatedResource();
      }

      LOG.info("slave stopped");
   }

}
