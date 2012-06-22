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
import com.dianping.swallow.producer.ProducerMode;

public class Producer {
   //变量定义
   private static Producer           instance;                                             //Producer实例
   private MQService                 remoteService;                                        //远程调用对象

   private HandlerAsynchroMode       asyncHandler;                                         //异步处理对象
   private HandlerSynchroMode        syncHandler;                                          //同步处理对象

   private HandlerUndeliverable      undeliverableMessageHandler;
   //常量定义
   private final String              producerVersion    = "0.6.0";
   private final Logger              log                = Logger.getLogger(Producer.class);


   //TODO 配置文件
   private ProducerMode              producerMode;                                         //Producer工作模式
   private Destination               destination;                                          //Producer消息目的
   private int                       threadPoolSize;                                       //异步处理对象的线程池大小
   private int                       asyncSendTimeout   = 5000;

   @SuppressWarnings("rawtypes")
   private final ProxyFactory        pigeon             = new ProxyFactory();

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
      pigeon.setTimeout(asyncSendTimeout);

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
            log.info("[Dest][" + msg.getDestination().getName() + "]" + msg.getContent());
         }
      };
      //向Swallow发送greet
      remoteService.sendMessage(new PktProducerGreet(producerVersion));
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
      return sendMessage(content, null, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    */
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

   /**
    * @return 返回远程调用接口
    */
   public MQService getRemoteService() {
      return remoteService;
   }

   /**
    * @return 返回Producer工作模式
    */
   public ProducerMode getProducerType() {
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
}
