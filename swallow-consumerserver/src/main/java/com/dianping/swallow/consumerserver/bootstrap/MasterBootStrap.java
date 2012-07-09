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
import com.dianping.swallow.consumerserver.netty.MessageServerHandler;
import com.dianping.swallow.consumerserver.worker.ConsumerWorkerManager;

public class MasterBootStrap {

   @SuppressWarnings("unused")
   private static final Logger LOG     = LoggerFactory.getLogger(MasterBootStrap.class);

   private static boolean      isSlave = false;

   /**
    * 启动Master
    */
   public static void main(String[] args) {
      //启动Cat
      Cat.initialize(new File("/data/appdatas/cat/client.xml"));

      ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "applicationContext-consumerserver.xml" });
      final ConsumerWorkerManager consumerWorkerManager = ctx.getBean(ConsumerWorkerManager.class);
      consumerWorkerManager.init(isSlave);
      //      try {
      //         Thread.sleep(consumerWorkerManager.getConfigManager().getWaitSlaveShutDown());//主机启动的时候睡眠一会，给时间给slave关闭。
      //      } catch (InterruptedException e) {
      //         LOG.error("thread InterruptedException", e);
      //      }

      // Configure the server.
      final ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         @Override
         public ChannelPipeline getPipeline() throws Exception {
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
      bootstrap.bind(new InetSocketAddress(consumerWorkerManager.getConfigManager().getMasterPort()));

      CloseMonitor closeMonitor = new CloseMonitor();
      int port = Integer.parseInt(System.getProperty("closeMonitorPort", "17555"));
      closeMonitor.start(port, new CloseHook() {

         @Override
         public void onClose() {
            consumerWorkerManager.close();
            MessageServerHandler.getChannelGroup().unbind();
            MessageServerHandler.getChannelGroup().close();
            MessageServerHandler.getChannelGroup().clear();
            bootstrap.releaseExternalResources();

         }

      });

   }

}
