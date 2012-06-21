package com.dianping.swallow.producer.impl;

import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.dianping.dpsf.api.ProxyFactory;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Message;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.common.util.MQService;
import com.dianping.swallow.producer.HandlerUndeliverable;
import com.dianping.swallow.producer.ProducerMode;

public class Producer {
   //变量定义
   private static Producer      instance;                                          //Producer实例
   private MQService            remoteService;                                     //远程调用对象

   private HandlerAsynchroMode  asyncHandler;                                      //异步处理对象
   private HandlerSynchroMode   syncHandler;                                       //同步处理对象

   private HandlerUndeliverable undeliverableMessageHandler;
   //常量定义
   private final String         producerVersion = "0.6.0";
   private final Logger         log             = Logger.getLogger(Producer.class);

   //TODO 设置or构造函数？
   private final ProducerMode   producerMode;                                      //Producer工作模式
   private final Destination    destination;                                       //Producer消息目的
   private final int            threadPoolSize;                                    //异步处理对象的线程池大小

   @SuppressWarnings("rawtypes")
   private final ProxyFactory   pigeon          = new ProxyFactory();

   private MQService initRemoteService() throws Exception {
      pigeon.setServiceName("remoteService");
      pigeon.setIface(MQService.class);
      pigeon.setSerialize("hessian");
      pigeon.setCallMethod("sync");
      pigeon.setTimeout(5000);

      //TODO 配置Lion支持
      pigeon.setUseLion(false);
      pigeon.setHosts("127.0.0.1:4000");
      pigeon.setWeight("1");

      pigeon.init();

      return (MQService) pigeon.getProxy();
   }

   //构造函数
   private Producer(ProducerMode producerType, Destination destination) throws Exception {
      remoteService = initRemoteService();
      this.producerMode = producerType;
      this.destination = destination;
      this.threadPoolSize = 10;
      //Producer工作模式
      switch (producerType) {
         case SYNCHRO_MODE:
            syncHandler = new HandlerSynchroMode(this);
            break;
         case ASYNCHRO_MODE:
            asyncHandler = new HandlerAsynchroMode(this);
            break;
      }
      //Message发送出错处理类
      undeliverableMessageHandler = new HandlerUndeliverable() {
         @Override
         public void handleUndeliverableMessage(Message msg) {
            log.info("[Dest][" + msg.getDestination().getName() + "]" + msg.getContent());
         }
      };
      //向Swallow发送greet
      remoteService.sendMessageWithoutReturn(new PktProducerGreet(producerVersion));
   }

   /**
    * 获取Producer单例，首次执行可指定Producer类型，再次指定Producer类型无效
    * 
    * @param producerMode producer工作模式
    * @param destination producer发送消息时的目的地
    * @return Producer全局单例
    * @throws Exception
    */
   public static synchronized Producer getInstance(ProducerMode producerMode, Destination destination) throws Exception {
      if (instance == null)
         instance = new Producer(producerMode, destination);
      return instance;
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    */
   public String sendMessage(Object content) {
      return sendMessage(content, null);
   }
   public String sendMessage(Object content, Properties properties) {
      String ret = null;

      //根据content生成SwallowMessage
      SwallowMessage swallowMsg = new SwallowMessage();

      swallowMsg.setContent(content);
      swallowMsg.setVersion(producerVersion);
      swallowMsg.setGeneratedTime(new Date());
      //		if(properties != null)	swallowMsg.setProperties(properties);

      //构造packet
      PktObjectMessage objMsg = new PktObjectMessage(destination, swallowMsg);
      switch (producerMode) {
         case SYNCHRO_MODE://同步模式
            ret = ((PktSwallowPACK) syncHandler.doSendMsg(objMsg)).getShaInfo();
            if (ret == null)
               handleUndeliverableMessage(objMsg);
            break;
         case ASYNCHRO_MODE://异步模式
            try {
               asyncHandler.doSendMsg(objMsg);
            } catch (FileQueueClosedException fqce) {
               log.info(fqce.toString());
               handleUndeliverableMessage(objMsg);
            }
            break;
      }
      return ret;
   }

   //处理发送失败的Handle
   private void handleUndeliverableMessage(Message msg) {
      try {
         undeliverableMessageHandler.handleUndeliverableMessage(msg);
      } catch (Exception e) {
         log.error("error processing undeliverable message", e);
      }
   }

   //getters && setters
   public MQService getSwallowAgency() {
      return remoteService;
   }

   public ProducerMode getProducerType() {
      return producerMode;
   }

   public String getProducerVersion() {
      return producerVersion;
   }

   public Destination getDestination() {
      return destination;
   }

   public int getSenderNum() {
      return threadPoolSize;
   }

}
