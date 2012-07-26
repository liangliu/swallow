package com.dianping.swallow.consumer.internal;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.internal.codec.JsonDecoder;
import com.dianping.swallow.common.internal.codec.JsonEncoder;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.util.NameCheckUtil;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerConfig;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.internal.config.ConfigManager;
import com.dianping.swallow.consumer.internal.netty.MessageClientHandler;

public class ConsumerImpl implements Consumer {

   private String                 consumerId;

   private Destination            dest;

   private ClientBootstrap        bootstrap;

   private MessageListener        listener;

   private InetSocketAddress      masterAddress;

   private InetSocketAddress      slaveAddress;

   private ConfigManager          configManager = ConfigManager.getInstance();

   private volatile boolean       closed        = false;

   private volatile AtomicBoolean started       = new AtomicBoolean(false);

   private ConsumerConfig         config;

   public boolean isClosed() {
      return closed;
   }

   public ConfigManager getConfigManager() {
      return configManager;
   }

   public ClientBootstrap getBootstrap() {
      return bootstrap;
   }

   public String getConsumerId() {
      return consumerId;
   }

   public void setConsumerId(String consumerId) {
      this.consumerId = consumerId;
   }

   public Destination getDest() {
      return dest;
   }

   public void setDest(Destination dest) {
      this.dest = dest;
   }

   public MessageListener getListener() {
      return listener;
   }

   @Override
   public void setListener(MessageListener listener) {
      this.listener = listener;
   }

   public ConsumerConfig getConfig() {
      return config;
   }

   public ConsumerImpl(Destination dest, ConsumerConfig config, InetSocketAddress masterAddress,
                       InetSocketAddress slaveAddress) {
      this(dest, null, config, masterAddress, slaveAddress);
   }

   public ConsumerImpl(Destination dest, String consumerId, ConsumerConfig config, InetSocketAddress masterAddress,
                       InetSocketAddress slaveAddress) {
      if (ConsumerType.NON_DURABLE == config.getConsumerType()) {//非持久类型，不能有consumerId
         if (consumerId != null) {
            throw new IllegalArgumentException("ConsumerId should be null when consumer type is NON_DURABLE");
         }
      } else {//持久类型，需要验证consumerId
         if (!NameCheckUtil.isConsumerIdValid(consumerId)) {
            throw new IllegalArgumentException("ConsumerId is invalid");
         }
      }
      this.dest = dest;
      this.consumerId = consumerId;
      this.config = config == null ? new ConsumerConfig() : config;
      this.masterAddress = masterAddress;
      this.slaveAddress = slaveAddress;
   }

   /**
    * 开始连接服务器，同时把连slave的线程启起来。
    */
   @Override
   public void start() {
      if (listener == null) {
         throw new IllegalArgumentException(
               "MessageListener is null, MessageListener should be set(use setListener()) before start.");
      }
      if (started.compareAndSet(false, true)) {
         init();
         //启动连接master的线程
         ConsumerThread master = new ConsumerThread();
         master.setBootstrap(bootstrap);
         master.setRemoteAddress(masterAddress);
         master.setInterval(configManager.getConnectMasterInterval());
         Thread masterThread = new Thread(master);
         masterThread.setName("masterThread");
         masterThread.start();
         //启动连接slave的线程
         ConsumerThread slave = new ConsumerThread();
         slave.setBootstrap(bootstrap);
         slave.setRemoteAddress(slaveAddress);
         slave.setInterval(configManager.getConnectSlaveInterval());
         Thread slaveThread = new Thread(slave);
         slaveThread.setName("slaveThread");
         slaveThread.start();
      }
   }

   //连接swollowC，获得bootstrap
   private void init() {
      bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
      final ConsumerImpl consumer = this;
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         @Override
         public ChannelPipeline getPipeline() throws Exception {
            MessageClientHandler handler = new MessageClientHandler(consumer);
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
            pipeline.addLast("jsonDecoder", new JsonDecoder(PktMessage.class));
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
            pipeline.addLast("jsonEncoder", new JsonEncoder(PktConsumerMessage.class));
            pipeline.addLast("handler", handler);
            return pipeline;
         }
      });
   }

   @Override
   public void close() {
      closed = true;
   }
}
