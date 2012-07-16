package com.dianping.swallow.producer.impl.internal;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.internal.util.NameCheckUtil;
import com.dianping.swallow.common.internal.util.ZipUtil;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerFactory;

/**
 * 实现Producer接口的类
 * 
 * @author Icloud
 */
public class ProducerImpl implements Producer {
   //变量定义
   private ProducerSwallowService remoteService;                                  //远程调用对象
   private HandlerAsynchroMode    asyncHandler;                                   //异步处理对象
   private HandlerSynchroMode     syncHandler;                                    //同步处理对象

   //常量定义
   private static final String    CAT_TYPE = "swallow";                           //cat监控使用的type
   private static final String    CAT_NAME = "produceMessage";                    //cat监控使用的name
   private static final Logger    logger   = Logger.getLogger(ProducerImpl.class); //日志
   private final String           producerIP;                                     //Producer IP地址
   private final String           producerVersion;                                //Producer版本号

   //Producer配置默认值
   private ProducerConfig         config;

   //Producer配置变量
   private Destination            destination;                                    //Producer消息目的

   /**
    * 构造函数
    * 
    * @param producerFactory Producer工厂类对象
    * @param dest Topic的Destination
    * @param pOptions producer配置选项
    * @throws TopicNameInvalidException topic名称非法时抛出此异常
    */
   public ProducerImpl(ProducerFactory producerFactory, Destination dest, ProducerConfig config)
         throws TopicNameInvalidException {

      //初始化Producer目的地
      if (!NameCheckUtil.isTopicNameValid(dest.getName()))
         throw new TopicNameInvalidException();
      destination = dest;
      this.config = config;

      //设置Producer的IP地址及版本号,设置远程调用
      this.producerIP = producerFactory.getProducerIP();
      this.producerVersion = producerFactory.getProducerVersion();
      this.remoteService = producerFactory.getRemoteService();

      //设置Producer工作模式
      switch (config.getMode()) {
         case SYNC_MODE:
            syncHandler = new HandlerSynchroMode(this);
            break;
         case ASYNC_MODE:
            asyncHandler = new HandlerAsynchroMode(this);
            break;
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
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties, String messageType)
         throws SendFailedException, NullContentException {
      String ret = null;
      if (content == null) {
         throw new NullContentException();
      }
      //根据content生成SwallowMessage
      SwallowMessage swallowMsg = null;
      Map<String, String> zipProperties = null;

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
         if (properties != null)
            swallowMsg.setProperties(properties);
         //压缩选项为真：对通过SwallowMessage类转换过的json字符串进行压缩，压缩成功时将compress=gzip写入InternalProperties，
         //               压缩失败时将compress=failed写入InternalProperties
         //压缩选项为假：不做任何操作，InternalProperties中将不存在key为zip的项
         if (config.isZipped()) {
            zipProperties = new HashMap<String, String>();
            try {
               swallowMsg.setContent(ZipUtil.zip(swallowMsg.getContent()));
               zipProperties.put("compress", "gzip");
            } catch (IOException e) {
               logger.error("[Compress message failed.]", e);
               zipProperties.put("compress", "failed");
            }
            swallowMsg.setInternalProperties(zipProperties);
         }

         //构造packet
         PktMessage pktMessage = new PktMessage(destination, swallowMsg);
         switch (config.getMode()) {
            case SYNC_MODE://同步模式
               try {
                  PktSwallowPACK pktSwallowPACK = null;
                  pktSwallowPACK = (PktSwallowPACK) syncHandler.doSendMsg(pktMessage);
                  if (pktSwallowPACK != null) {
                     ret = pktSwallowPACK.getShaInfo();
                  }
               } catch (ServerDaoException e) {
                  logger.error("[Save to DB failed.]", e);
                  throw new SendFailedException("save to DB failed.");
               } catch (RemoteServiceDownException e) {
                  logger.error("[Remote call failed.]", e);
                  throw new SendFailedException("remote call failed.");
               }
               break;
            case ASYNC_MODE://异步模式
               try {
                  asyncHandler.doSendMsg(pktMessage);
               } catch (FileQueueClosedException e) {
                  logger.error("[Add to filequeue failed.]", e);
                  throw new SendFailedException("add to filequeue failed.");
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
   public ProducerSwallowService getRemoteService() {
      return remoteService;
   }

   /**
    * @return 返回ProducerConfig
    */
   public ProducerConfig getProducerConfig() {
      return config;
   }

   /**
    * @return 返回producer消息目的地
    */
   public Destination getDestination() {
      return destination;
   }

}
