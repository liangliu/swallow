package com.dianping.swallow.producer.impl.internal;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.internal.packet.PktMessage;
import com.dianping.swallow.common.internal.packet.PktSwallowPACK;
import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.internal.util.ZipUtil;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.ProducerHandler;

/**
 * 实现Producer接口的类
 * 
 * @author tong.song
 */
public class ProducerImpl implements Producer {
   //常量定义
   private static final Logger          LOGGER = Logger.getLogger(ProducerImpl.class); //日志

   //变量定义
   private final Destination            destination;                                  //Producer消息目的
   private final ProducerConfig         producerConfig;
   private final String                 producerIP;                                   //Producer IP地址
   private final String                 producerVersion;                              //Producer版本号
   private final ProducerSwallowService remoteService;
   private final int                    punishTimeout;
   private final ProducerHandler        producerHandler;

   /**
    * @param destination 此Producer发送消息的目的地
    * @param producerConfig Producer的配置信息
    * @param producerIP 本机IP地址
    * @param producerVersion Producer版本号
    * @param remoteService 远程调用服务接口
    * @param punishTimeout 远程调用超时
    */
   public ProducerImpl(Destination destination, ProducerConfig producerConfig, String producerIP,
                       String producerVersion, ProducerSwallowService remoteService, int punishTimeout) {
      if (producerConfig != null) {
         this.producerConfig = producerConfig;
      } else {
         LOGGER.warn("config is null, use default settings.");
         this.producerConfig = new ProducerConfig();
      }

      //设置Producer的IP地址及版本号,设置远程调用
      this.destination = destination;
      this.producerIP = producerIP;
      this.producerVersion = producerVersion;
      this.remoteService = remoteService;
      this.punishTimeout = punishTimeout;

      //设置Producer工作模式
      switch (this.producerConfig.getMode()) {
         case SYNC_MODE:
            producerHandler = new HandlerSynchroMode(this);
            break;
         case ASYNC_MODE:
            producerHandler = new HandlerAsynchroMode(this);
            break;
         default:
            producerHandler = new HandlerSynchroMode(this);
            break;
      }
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @return 异步模式返回null，同步模式返回将content转化为json字符串后，与其对应的SHA-1签名
    * @throws SendFailedException 发送失败则抛出此异常
    */
   @Override
   public String sendMessage(Object content) throws SendFailedException {
      return sendMessage(content, null, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws SendFailedException 发送失败则抛出此异常
    */
   @Override
   public String sendMessage(Object content, String messageType) throws SendFailedException {
      return sendMessage(content, null, messageType);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param properties 消息属性，留作后用
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws SendFailedException 发送失败则抛出此异常
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties) throws SendFailedException {
      return sendMessage(content, properties, null);
   }

   /**
    * 将Object类型的content发送到指定的Destination
    * 
    * @param content 待发送的消息内容
    * @param properties 消息属性，留作后用
    * @param messageType 消息类型，用于消息过滤
    * @return 异步模式返回null，同步模式返回content的SHA-1字符串
    * @throws SendFailedException 发送失败则抛出此异常
    */
   @Override
   public String sendMessage(Object content, Map<String, String> properties, String messageType)
         throws SendFailedException {
      if (content == null) {
         throw new IllegalArgumentException("Message content can not be null.");
      }
      //根据content生成SwallowMessage
      SwallowMessage swallowMsg = new SwallowMessage();
      Map<String, String> zipProperties = null;

      //使用CAT监控处理消息的时间
      Transaction t = Cat.getProducer().newTransaction("SendMessage", destination.getName());
      Event event = Cat.getProducer().newEvent("Message", "Payload");
      try {
         //根据content生成SwallowMessage
         swallowMsg.setContent(content);
         swallowMsg.setVersion(producerVersion);
         swallowMsg.setGeneratedTime(new Date());
         swallowMsg.setSourceIp(producerIP);

         if (messageType != null) {
            swallowMsg.setType(messageType);
         }
         if (properties != null) {
            //            Iterator propIter = properties.entrySet().iterator();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
               if (!(entry.getKey() instanceof String)
                     || (entry.getValue() != null && !(entry.getValue() instanceof String))) {
                  throw new IllegalArgumentException("Type of properties should be Map<String, String>.");
               }
            }
            swallowMsg.setProperties(properties);
         }
         //压缩选项为真：对通过SwallowMessage类转换过的json字符串进行压缩，压缩成功时将compress=gzip写入InternalProperties，
         //               压缩失败时将compress=failed写入InternalProperties
         //压缩选项为假：不做任何操作，InternalProperties中将不存在key为zip的项
         if (producerConfig.isZipped()) {
            zipProperties = new HashMap<String, String>();
            try {
               swallowMsg.setContent(ZipUtil.zip(swallowMsg.getContent()));
               zipProperties.put("compress", "gzip");
            } catch (Exception e) {
               LOGGER.warn("Compress message failed.Content=" + swallowMsg.getContent(), e);
               zipProperties.put("compress", "failed");
            }
            swallowMsg.setInternalProperties(zipProperties);
         }

         //构造packet
         PktMessage pktMessage = new PktMessage(destination, swallowMsg);

         String ret = null;
         switch (producerConfig.getMode()) {
            case SYNC_MODE://同步模式
               PktSwallowPACK pktSwallowPACK = (PktSwallowPACK) producerHandler.doSendMsg(pktMessage);
               if (pktSwallowPACK != null) {
                  ret = pktSwallowPACK.getShaInfo();
               }
               break;
            case ASYNC_MODE://异步模式
               producerHandler.doSendMsg(pktMessage);
               break;
         }
         //CAT
         event.setStatus(Message.SUCCESS);
         t.setStatus(Message.SUCCESS);

         return ret;
      } catch (SendFailedException e) {
         event.addData(swallowMsg.toKeyValuePairs());
         event.setStatus(e);
         t.setStatus(e);
         Cat.getProducer().logError(e);
         throw e;
      } catch (RuntimeException e) {
         event.addData(swallowMsg.toKeyValuePairs());
         event.setStatus(e);
         t.setStatus(e);
         Cat.getProducer().logError(e);
         throw e;
      } finally {
         event.complete();
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
      return producerConfig;
   }

   /**
    * @return 返回producer消息目的地
    */
   public Destination getDestination() {
      return destination;
   }

   /**
    * @return 远程调用超时
    */
   public int getPunishTimeout() {
      return punishTimeout;
   }
}
