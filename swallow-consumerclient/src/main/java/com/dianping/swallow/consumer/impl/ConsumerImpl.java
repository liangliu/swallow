package com.dianping.swallow.consumer.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.dianping.swallow.consumer.internal.ConsumerSlaveThread;
import com.dianping.swallow.consumer.internal.config.ConfigManager;
import com.dianping.swallow.consumer.internal.netty.MessageClientHandler;

public class ConsumerImpl implements Consumer{

   private static final Logger LOG                          = LoggerFactory.getLogger(ConsumerImpl.class);

   private String              consumerId;

   private Destination         dest;

   private ClientBootstrap     bootstrap;

   private MessageListener     listener;

   private InetSocketAddress   masterAddress;

   private InetSocketAddress   slaveAddress;

   private ConfigManager       configManager                = ConfigManager.getInstance();

   private volatile boolean             closed                    = false;
   
   private volatile AtomicBoolean started = new AtomicBoolean(false);
   
   private ConsumerConfig config;

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

   public void setListener(MessageListener listener) {
      this.listener = listener;
   }

   public ConsumerConfig getConfig() {
      return config;
   }

   public ConsumerImpl(Destination dest, ConsumerConfig config, InetSocketAddress masterAddress, InetSocketAddress slaveAddress) {
      this(dest, null, config, masterAddress, slaveAddress);
   }
   
   public ConsumerImpl(Destination dest, String consumerId, ConsumerConfig config, InetSocketAddress masterAddress, InetSocketAddress slaveAddress) {
      if(ConsumerType.NON_DURABLE == config.getConsumerType() && consumerId != null) {
         throw new IllegalArgumentException("ConsumerId should be null when consumer type is NON_DURABLE");
      }
      if(consumerId != null && !NameCheckUtil.isConsumerIdValid(consumerId)) {
         throw new IllegalArgumentException("ConsumerId is invalid");
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
   public void start() {
      if(listener == null){
         LOG.error("MessageListener is null");
         return;
      }
      if(started.compareAndSet(false, true)) {
         init();
         ConsumerSlaveThread slave = new ConsumerSlaveThread();
         slave.setBootstrap(bootstrap);
         slave.setSlaveAddress(slaveAddress);
         slave.setConfigManager(configManager);
         Thread slaveThread = new Thread(slave);
         slaveThread.start();
         while (true) {
            synchronized (bootstrap) {
               try {
                  ChannelFuture future = bootstrap.connect(masterAddress);
                  future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞！     
               } catch (RuntimeException e) {
                  LOG.error("Unexpected exception", e);
               }
            }
            try {
               Thread.sleep(configManager.getConnectMasterInterval());
            } catch (InterruptedException e) {
               LOG.error("thread InterruptedException", e);
            }
         }
      }
   }

   //连接swollowC，获得bootstrap
   private void init() {
      bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
      final ConsumerImpl cc = this;
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         @Override
         public ChannelPipeline getPipeline() throws Exception {
            MessageClientHandler handler = new MessageClientHandler(cc);
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
