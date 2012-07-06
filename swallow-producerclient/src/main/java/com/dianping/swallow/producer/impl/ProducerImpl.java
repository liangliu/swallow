package com.dianping.swallow.producer.impl;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.packet.PktProducerGreet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.common.util.IPUtil;
import com.dianping.swallow.common.util.NameCheckUtil;
import com.dianping.swallow.common.util.ZipUtil;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;

public class ProducerImpl implements Producer {
   //变量定义
   private MQService                 remoteService;                                                   //远程调用对象
   private HandlerAsynchroMode       asyncHandler;                                                    //异步处理对象
   private HandlerSynchroMode        syncHandler;                                                     //同步处理对象

   //常量定义
   private static final String       CAT_TYPE                 = "swallow";                            //cat监控使用的type
   private static final String       CAT_NAME                 = "produceMessage";                     //cat监控使用的name
   private final String              producerVersion          = "0.6.0";                              //Producer版本号
   private final String              producerIP               = IPUtil.getFirstNoLoopbackIP4Address(); //Producer IP地址
   private static final Logger       logger                   = Logger.getLogger(ProducerImpl.class); //日志

   //Producer配置默认值
   private static final ProducerMode DEFAULT_PRODUCER_MODE    = ProducerMode.SYNC_MODE;
   private static final int          DEFAULT_THREAD_POOL_SIZE = 10;
   private static final boolean      DEFAULT_CONTINUE_SEND    = false;
   private static final int          DEFAULT_RETRY_TIMES      = 5;

   //Producer配置变量
   private Destination               destination;                                                     //Producer消息目的
   private ProducerMode              producerMode             = DEFAULT_PRODUCER_MODE;                //Producer工作模式
   private int                       retryTimes               = DEFAULT_RETRY_TIMES;                  //Producer重试次数
   private int                       threadPoolSize           = DEFAULT_THREAD_POOL_SIZE;             //异步处理对象的线程池大小
   private boolean                   continueSend             = DEFAULT_CONTINUE_SEND;                //异步模式是否允许续传

   /**
    * 构造函数
    * 
    * @param remoteService 远程调用服务
    * @param dest topic的名称
    * @param pOptions producer选项
    * @throws TopicNameInvalidException topic名称非法//topic名称只能由字母、数字、下划线组成
    * @throws RemoteServiceDownException 初始化远程连接失败
    */
   ProducerImpl(MQService remoteService, Destination dest, Map<ProducerOptionKey, Object> pOptions)
         throws TopicNameInvalidException, RemoteServiceDownException {

      //初始化Producer目的地
      if (!NameCheckUtil.isTopicNameValid(dest.getName()))
         throw new TopicNameInvalidException();
      destination = dest;

      //初始化Produce选项
      if (pOptions != null) {
         Object obj = null;
         if (pOptions.containsKey(ProducerOptionKey.PRODUCER_MODE)) {
            obj = pOptions.get(ProducerOptionKey.PRODUCER_MODE);
            if (ProducerMode.class.isInstance(obj)) {
               producerMode = (ProducerMode) obj;
            } else {
               logger.warn("[pOptions]:[ProducerMode's format is incorrect, use default value: ProducerMode.SYNC_MODE]");
            }
         }
         if (pOptions.containsKey(ProducerOptionKey.RETRY_TIMES)) {
            obj = pOptions.get(ProducerOptionKey.RETRY_TIMES);
            if (Integer.class.isInstance(obj)) {
               if ((Integer) obj >= 0) {
                  retryTimes = (Integer) obj;
               } else {
                  logger.warn("[pOptions]:[Retry times should be more than 0, use default value: 0]");
                  retryTimes = 0;
               }
            } else {
               logger.warn("[pOptions]:[RetryTimes's format is incorrect, use default value: 5]");
            }
         }
         if (producerMode == ProducerMode.ASYNC_MODE) {
            if (pOptions.containsKey(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND)) {
               obj = pOptions.get(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND);
               if (Boolean.class.isInstance(obj)) {
                  continueSend = (Boolean) obj;
               } else {
                  logger.warn("[pOptions]:[ContinueSend's format is incorrect, use default value: false]");
               }
            }
            if (pOptions.containsKey(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE)) {
               obj = pOptions.get(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE);
               if (Integer.class.isInstance(obj)) {
                  if ((Integer) obj >= 1) {
                     threadPoolSize = (Integer) obj;
                  } else {
                     logger.warn("[pOptions]:[Threadpool size should be bigger than 1, use default value: 1]");
                     threadPoolSize = 1;
                  }
               } else {
                  logger.warn("[pOptions]:[ThreadPoolSize's format is incorrect, use default value: 5]");
               }
            }
         }
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
         remoteService.sendMessage(new PktProducerGreet(producerVersion, producerIP));
      } catch (Exception e) {
         logger.error(
               "[Can not connect to remote service, configuration on LION is incorrect or network is instability now.]",
               e);
         throw new RemoteServiceDownException();
      }
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws ServerDaoException 只存在于同步模式，保存message到数据库失败
    * @throws FileQueueClosedException 只存在于异步模式，保存message到队列失败
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   @Override
   public String sendMessage(Object content) throws ServerDaoException, FileQueueClosedException,
         RemoteServiceDownException, NullContentException {
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
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   @Override
   public String sendMessage(Object content, String messageType) throws ServerDaoException, FileQueueClosedException,
         RemoteServiceDownException, NullContentException {
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
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties) throws ServerDaoException,
         FileQueueClosedException, RemoteServiceDownException, NullContentException {
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
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties, String messageType)
         throws ServerDaoException, FileQueueClosedException, RemoteServiceDownException, NullContentException {
      String ret = null;
      if (content == null) {
         throw new NullContentException();
      }
      //根据content生成SwallowMessage
      SwallowMessage swallowMsg = new SwallowMessage();

      //使用CAT监控处理消息的时间
      Transaction t = Cat.getProducer().newTransaction(CAT_TYPE, CAT_NAME);
      try {
         Event event = Cat.getProducer().newEvent(CAT_TYPE, CAT_NAME);

         //根据content生成SwallowMessage
         swallowMsg = new SwallowMessage();
         swallowMsg.setContent(content);
         swallowMsg.setVersion(producerVersion);
         swallowMsg.setGeneratedTime(new Date());
         swallowMsg.setSourceIp(producerIP);

         if (messageType != null)
            swallowMsg.setType(messageType);
         if (properties != null) {
            //如果需要压缩内容，properties设置zip=true
            //PS：压缩内容并非用户传入的Object，而是通过SwallowMessage类转换过的json字符串，压缩失败时，将zip置为false
            if (properties.containsKey("zip") && properties.get("zip").equals("true")) {
               try {
                  swallowMsg.setContent(ZipUtil.zip(swallowMsg.getContent()));
               } catch (IOException e) {
                  logger.error("[Compress message failed.]", e);
                  properties.put("zip", "false");
               }
            }
            swallowMsg.setProperties(properties);
         }

         //构造packet
         PktMessage pktMessage = new PktMessage(destination, swallowMsg);
         switch (producerMode) {
            case SYNC_MODE://同步模式
               try {
                  PktSwallowPACK pktSwallowPACK = null;
                  pktSwallowPACK = (PktSwallowPACK) syncHandler.doSendMsg(pktMessage);
                  if (pktSwallowPACK != null) {
                     ret = pktSwallowPACK.getShaInfo();
                  }
               } catch (ServerDaoException e) {
                  logger.error("[Can not save message to DB, DB is busy or connection to DB is down.]", e);
                  throw e;
               } catch (RemoteServiceDownException e) {
                  logger.error(
                        "[Can not connect to remote service, configuration on LION is incorrect or network is instability now.]",
                        e);
                  throw e;
               }
               break;
            case ASYNC_MODE://异步模式
               try {
                  asyncHandler.doSendMsg(pktMessage);
               } catch (FileQueueClosedException e) {
                  logger.error(
                        "[Can not save message to FileQueue, please contact to Swallow-Team for more information.]", e);
                  throw e;
               }
               break;
         }

         event.addData(swallowMsg.toString());
         event.setStatus(Event.SUCCESS);
         event.complete();
         t.setStatus(Transaction.SUCCESS);

         return ret;
      } finally {
         t.complete();
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
