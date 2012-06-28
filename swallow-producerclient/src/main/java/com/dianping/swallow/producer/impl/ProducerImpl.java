package com.dianping.swallow.producer.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.ProducerUtils;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;

public class ProducerImpl implements Producer {
   //变量定义
   private MQService                 remoteService;                                                  //远程调用对象
   private HandlerAsynchroMode       asyncHandler;                                                   //异步处理对象
   private HandlerSynchroMode        syncHandler;                                                    //同步处理对象

   //常量定义
   private final String              producerVersion          = "0.6.0";                             //Producer版本号
   private static final Logger       logger                   = Logger.getLogger(ProducerImpl.class); //日志

   //Producer配置默认值
   private static final ProducerMode DEFAULT_PRODUCER_MODE    = ProducerMode.SYNC_MODE;
   private static final int          DEFAULT_THREAD_POOL_SIZE = 10;
   private static final boolean      DEFAULT_CONTINUE_SEND    = false;
   private static final int          DEFAULT_RETRY_TIMES      = 5;

   //Producer配置变量
   private Destination               destination;                                                    //Producer消息目的
   private ProducerMode              producerMode             = DEFAULT_PRODUCER_MODE;               //Producer工作模式
   private int                       threadPoolSize           = DEFAULT_THREAD_POOL_SIZE;            //异步处理对象的线程池大小
   private boolean                   continueSend             = DEFAULT_CONTINUE_SEND;               //异步模式是否允许续传
   private int                       retryTimes               = DEFAULT_RETRY_TIMES;                 //异步模式重试次数

   /**
    * 构造函数
    * 
    * @param remoteService 远程调用服务
    * @param topicName topic的名称
    * @param pOptions producer选项
    * @throws TopicNameInvalidException topic名称非法//topic名称只能由以下字符组成：[a-z]、[A-Z]、[_]、[.]、[0-9]
    */
   ProducerImpl(MQService remoteService, String topicName, Map<ProducerOptionKey, Object> pOptions)
         throws TopicNameInvalidException {
      //初始化Producer参数
      if (!ProducerUtils.isTopicNameValid(topicName))
         throw new TopicNameInvalidException();

      destination = Destination.topic(topicName);

      if (pOptions != null) {
         producerMode = pOptions.containsKey(ProducerOptionKey.PRODUCER_MODE) ? ((ProducerMode) pOptions
               .get(ProducerOptionKey.PRODUCER_MODE) == ProducerMode.ASYNC_MODE ? ProducerMode.ASYNC_MODE
               : DEFAULT_PRODUCER_MODE) : DEFAULT_PRODUCER_MODE;
         threadPoolSize = pOptions.containsKey(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE) ? (Integer) pOptions
               .get(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE) : DEFAULT_THREAD_POOL_SIZE;
         continueSend = pOptions.containsKey(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND) ? (Boolean) pOptions
               .get(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND) : DEFAULT_CONTINUE_SEND;
         retryTimes = pOptions.containsKey(ProducerOptionKey.ASYNC_RETRY_TIMES) ? (Integer) pOptions
               .get(ProducerOptionKey.ASYNC_RETRY_TIMES) : DEFAULT_RETRY_TIMES;
      }
      //初始化远程调用
      this.remoteService = remoteService;
      //设置Producer工作模式
      switch (producerMode) {
         case SYNC_MODE:
            syncHandler = new HandlerSynchroMode(this);
            break;
         case ASYNC_MODE:
            asyncHandler = new HandlerAsynchroMode(this);
            break;
      }
      //向Swallow发送greet
      try {
         remoteService.sendMessage(new PktProducerGreet(producerVersion));
      } catch (ServerDaoException e) {
         e.printStackTrace();
      }
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws ServerDaoException 只存在于同步模式，保存message到数据库失败
    * @throws FileQueueClosedException 只存在于异步模式，保存message到队列失败
    */
   @Override
   public String sendMessage(Object content) throws ServerDaoException, FileQueueClosedException {
      return sendMessage(content, null, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws ServerDaoException 只存在于同步模式，保存message到数据库失败
    * @throws FileQueueClosedException 只存在于异步模式，保存message到队列失败
    */
   @Override
   public String sendMessage(Object content, String messageType) throws ServerDaoException, FileQueueClosedException {
      return sendMessage(content, null, messageType);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param properties 消息属性，留作后用
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws ServerDaoException 只存在于同步模式，保存message到数据库失败
    * @throws FileQueueClosedException 只存在于异步模式，保存message到队列失败
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties) throws ServerDaoException,
         FileQueueClosedException {
      return sendMessage(content, properties, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param properties 消息属性，留作后用
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws ServerDaoException 只存在于同步模式，保存message到数据库失败
    * @throws FileQueueClosedException 只存在于异步模式，保存message到队列失败
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties, String messageType)
         throws ServerDaoException, FileQueueClosedException {

      String ret = null;
      //根据content生成SwallowMessage
      SwallowMessage swallowMsg = new SwallowMessage();

      swallowMsg.setContent(content);
      swallowMsg.setVersion(producerVersion);
      swallowMsg.setGeneratedTime(new Date());
      try {
         swallowMsg.setSourceIp(InetAddress.getLocalHost().getHostAddress());
      } catch (UnknownHostException e1) {
         e1.printStackTrace();
      }
      if (properties != null)
         swallowMsg.setProperties(properties);
      if (messageType != null)
         swallowMsg.setType(messageType);

      //构造packet
      PktMessage pktMessage = new PktMessage(destination, swallowMsg);
      switch (producerMode) {
         case SYNC_MODE://同步模式
            try {
               ret = ((PktSwallowPACK) syncHandler.doSendMsg(pktMessage)).getShaInfo();
            } catch (ServerDaoException e) {
               logger.log(Level.ERROR, "[SendMessage]:[Message sent failed.]", e.getCause());
               throw e;
            }
            break;
         case ASYNC_MODE://异步模式
            try {
               asyncHandler.doSendMsg(pktMessage);
            } catch (FileQueueClosedException e) {
               logger.log(Level.ERROR, "[SendMessage]:[Message sent failed.]", e.getCause());
               throw e;
            }
            break;
      }
      return ret;
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
    * @return 返回Producer版本号
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
    * @return 返回异步模式是否续传
    */
   public boolean isContinueSend() {
      return continueSend;
   }

   /**
    * @return 返回异步模式重试次数
    */
   public int getRetryTimes() {
      return retryTimes;
   }
}
