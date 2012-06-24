package com.dianping.swallow.producer.impl;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dianping.dpsf.api.ProxyFactory;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.Message;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.Destination;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.producer.HandlerUndeliverable;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerMode;

public class ProducerImpl implements Producer {
   //变量定义
   private static ProducerImpl  instance;                                              //Producer实例
   private MQService            remoteService;                                         //远程调用对象

   private HandlerAsynchroMode  asyncHandler;                                          //异步处理对象
   private HandlerSynchroMode   syncHandler;                                           //同步处理对象

   private HandlerUndeliverable undeliverableMessageHandler;
   //常量定义
   private final String         producerVersion = "0.6.0";
   private final Logger         logger          = Logger.getLogger(ProducerImpl.class);

   //和配置文件对应的变量
   private final ProducerMode   producerMode;                                          //Producer工作模式
   private final Destination    destination;                                           //Producer消息目的
   private final int            threadPoolSize;                                        //异步处理对象的线程池大小
   private final int            remoteServiceTimeout;                                  //远程调用超时
   private final boolean        continueSend;                                          //异步模式是否允许续传

   @SuppressWarnings("rawtypes")
   private final ProxyFactory   pigeon          = new ProxyFactory();

   /**
    * 初始化远程调用服务，如果远程服务端连接失败，抛出异常
    * 
    * @return 远程调用服务的借口
    * @throws Exception 远程调用服务失败
    */
   private MQService initRemoteService() throws Exception {
      pigeon.setServiceName("remoteService");
      pigeon.setIface(MQService.class);
      pigeon.setSerialize("hessian");
      pigeon.setCallMethod("sync");
      pigeon.setTimeout(getRemoteServiceTimeout());

      //TODO 配置Lion支持
      pigeon.setUseLion(false);
      pigeon.setHosts("127.0.0.1:4000");
      pigeon.setWeight("1");

      pigeon.init();

      return (MQService) pigeon.getProxy();
   }

   /**
    * Producer构造函数
    * 
    * @param producerConfigure Producer配置类
    * @throws Exception
    */
   private ProducerImpl(ProducerConfigure producerConfigure) throws Exception {
      //读取配置文件
      this.producerMode = (producerConfigure.getProducerModeStr().equals("async")) ? ProducerMode.ASYNC_MODE
            : ProducerMode.SYNC_MODE;
      this.destination = Destination.topic(producerConfigure.getDestinationName());
      this.threadPoolSize = producerConfigure.getThreadPoolSize();
      this.remoteServiceTimeout = producerConfigure.getRemoteServiceTimeout();
      this.continueSend = producerConfigure.isContinueSend();
      //初始化远程调用
      remoteService = initRemoteService();
      //Producer工作模式
      switch (producerMode) {
         case SYNC_MODE:
            syncHandler = new HandlerSynchroMode(this);
            break;
         case ASYNC_MODE:
            asyncHandler = new HandlerAsynchroMode(this);
            break;
      }
      //Message发送出错处理类
      undeliverableMessageHandler = new HandlerUndeliverable() {
         @Override
         public void handleUndeliverableMessage(Message msg) {
            logger.info("[Dest][" + msg.getDestination().getName() + "]" + msg.getContent());
         }
      };
      //向Swallow发送greet
      remoteService.sendMessage(new PktProducerGreet(producerVersion));
   }

   /**
    * 获得默认配置的Producer单例，无参数
    * 
    * @return Producer单例
    * @throws Exception
    */
   public static ProducerImpl getInstance() throws Exception {
      return doGetInstance(null);
   }

   /**
    * 获得指定配置文件的Producer单例，如果配置文件格式错误或无法加载，则返回默认配置的Producer
    * 
    * @param configFile Producer的配置文件
    * @return Producer单例
    * @throws Exception
    */
   public static ProducerImpl getInstance(String configFile) throws Exception {
      return doGetInstance(configFile);
   }

   /**
    * 实际产生Producer单例的函数
    * 
    * @param configFile Producer配置文件名，可以为null
    * @return Producer单例
    * @throws Exception
    */
   private synchronized static ProducerImpl doGetInstance(String configFile) throws Exception {
      if (instance == null) {
         if (configFile == null)
            instance = new ProducerImpl(new ProducerConfigure());
         else
            instance = new ProducerImpl(new ProducerConfigure(configFile));
      }
      return instance;
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    */
   @Override
   public String sendMessage(Object content) {
      return sendMessage(content, null, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    */
   @Override
   public String sendMessage(Object content, String messageType) {
      return sendMessage(content, null, messageType);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param properties 消息属性，留作后用
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties) {
      return sendMessage(content, properties, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param properties 消息属性，留作后用
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties, String messageType) {

      String ret = null;
      //根据content生成SwallowMessage
      SwallowMessage swallowMsg = new SwallowMessage();

      swallowMsg.setContent(content);
      swallowMsg.setVersion(producerVersion);
      swallowMsg.setGeneratedTime(new Date());
      if (properties != null)
         swallowMsg.setProperties(properties);
      if (messageType != null)
         swallowMsg.setType(messageType);

      //构造packet
      PktObjectMessage objMsg = new PktObjectMessage(destination, swallowMsg);
      switch (producerMode) {
         case SYNC_MODE://同步模式
            ret = ((PktSwallowPACK) syncHandler.doSendMsg(objMsg)).getShaInfo();
            if (ret == null)
               handleUndeliverableMessage(objMsg);
            break;
         case ASYNC_MODE://异步模式
            try {
               asyncHandler.doSendMsg(objMsg);
            } catch (FileQueueClosedException fqce) {
               logger.info(fqce.toString());
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
         logger.error("error processing undeliverable message", e);
      }
   }

   /**
    * @return 返回远程调用接口
    */
   public MQService getRemoteService() {
      return remoteService;
   }

   /**
    * @return 返回Producer工作模式
    */
   public ProducerMode getProducerMode() {
      return producerMode;
   }

   /**
    * @return返回Producer版本号
    */
   public String getProducerVersion() {
      return producerVersion;
   }

   /**
    * @return 返回producer消息目的地
    */
   public Destination getDestination() {
      return destination;
   }

   /**
    * @return 返回异步模式时线程池大小
    */
   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * @return 返回远程调用超时
    */
   public int getRemoteServiceTimeout() {
      return remoteServiceTimeout;
   }

   /**
    * @return 返回异步模式是否续传
    */
   public boolean isContinueSend() {
      return continueSend;
   }

}
