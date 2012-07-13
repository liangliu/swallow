package com.dianping.swallow.consumer.impl;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.Executors;

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
import com.dianping.swallow.common.internal.config.DynamicConfig;
import com.dianping.swallow.common.internal.config.impl.LionDynamicConfig;
import com.dianping.swallow.common.internal.packet.PktConsumerMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.consumer.Consumer;
import com.dianping.swallow.consumer.ConsumerConfig;
import com.dianping.swallow.consumer.MessageListener;
import com.dianping.swallow.consumer.internal.ConsumerSlaveThread;
import com.dianping.swallow.consumer.internal.config.ConfigManager;
import com.dianping.swallow.consumer.internal.netty.MessageClientHandler;

public class ConsumerImpl implements Consumer{

   private static final Logger LOG                          = LoggerFactory.getLogger(ConsumerImpl.class);

   private static final String LION_CONFIG_FILENAME         = "swallow-consumerclient-lion.properties";

   private static final String TOPICNAME_DEFAULT            = "default";

   private String              consumerId;

   private Destination         dest;

   private ClientBootstrap     bootstrap;

   private MessageListener     listener;

   private final static String LION_KEY_CONSUMER_SERVER_URI = "swallow.consumer.consumerServerURI";

   private ConsumerType        consumerType                 = ConsumerType.AT_MOST_ONCE;

   private InetSocketAddress   masterAddress;

   private InetSocketAddress   slaveAddress;

   private ConfigManager       configManager                = ConfigManager.getInstance();

   private boolean             needClose                    = false;
   
   private ConsumerConfig config;

   public boolean getNeedClose() {
      return needClose;
   }

   public ConfigManager getConfigManager() {
      return configManager;
   }

   public void setNeedClose(boolean needClose) {
      this.needClose = needClose;
   }

   public ConsumerType getConsumerType() {
      return consumerType;
   }

   public void setConsumerType(ConsumerType consumerType) {
      this.consumerType = consumerType;
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

   public ConsumerImpl(Destination dest, String topicName, ConsumerConfig config) {
      this.dest = Destination.topic(topicName);
      String swallowCAddress = getSwallowCAddress(topicName);
      string2Address(swallowCAddress);
   }

   /**
    * 开始连接服务器，同时把连slave的线程启起来。
    */
   public void start() {
      init();
      ConsumerSlaveThread slave = new ConsumerSlaveThread();
      slave.setBootstrap(bootstrap);
      slave.setSlaveAddress(slaveAddress);
      slave.setConfigManager(configManager);
      Thread slaveThread = new Thread(slave);
      slaveThread.start();
      while (true) {
         synchronized (bootstrap) {
            ChannelFuture future = bootstrap.connect(masterAddress);
            try {
               future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞！     
            } catch (Exception e) {
               LOG.info("something wrong!", e);
            }
         }
         try {
            Thread.sleep(configManager.getConnectMasterInterval());
         } catch (InterruptedException e) {
            LOG.error("thread InterruptedException", e);
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

   private void string2Address(String swallowCAddress) {
      String[] ipAndPorts = swallowCAddress.split(",");
      String masterIp = ipAndPorts[0].split(":")[0];
      int masterPort = Integer.parseInt(ipAndPorts[0].split(":")[1]);
      String slaveIp = ipAndPorts[1].split(":")[0];
      int slavePort = Integer.parseInt(ipAndPorts[1].split(":")[1]);
      masterAddress = new InetSocketAddress(masterIp, masterPort);
      slaveAddress = new InetSocketAddress(slaveIp, slavePort);

   }

   private String getSwallowCAddress(String topicName) {
      DynamicConfig dynamicConfig = new LionDynamicConfig(LION_CONFIG_FILENAME);
      String lionValue = dynamicConfig.get(LION_KEY_CONSUMER_SERVER_URI);
      return getAddressByParseLionValue(lionValue, topicName);
   }

   private String getAddressByParseLionValue(String lionValue, String topicName) {
      String swallowAddress = null;
      label: for (String topicNameToAddress : lionValue.split(";")) {
         String[] splits = topicNameToAddress.split("=");
         String address = splits[1];
         String topicNameStr = splits[0];
         for (String tempTopicName : topicNameStr.split(",")) {
            if (TOPICNAME_DEFAULT.equals(tempTopicName)) {
               swallowAddress = address;
            }
            if (topicName.equals(tempTopicName)) {
               swallowAddress = address;
               break label;
            }
         }
      }
      return swallowAddress;
   }
}
