package com.dianping.swallow.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerConfig {
   Logger                           logger                             = LoggerFactory.getLogger(ProducerConfig.class);

   public static final int          MAX_THREADPOOL_SIZE                = 100;
   public static final ProducerMode DEFAULT_PRODUCER_MODE              = ProducerMode.SYNC_MODE;
   public static final int          DEFAULT_RETRY_TIMES                = 5;
   public static final boolean      DEFAULT_ZIPPED                     = false;
   public static final int          DEFAULT_THREADPOOL_SIZE            = 5;
   public static final boolean      DEFAULT_SEND_MSG_LEFT_LAST_SESSION = false;
   
   private ProducerMode             mode                               = DEFAULT_PRODUCER_MODE;
   private int                      retryTimes                         = DEFAULT_RETRY_TIMES;
   private boolean                  zipped                             = DEFAULT_ZIPPED;
   private int                      threadPoolSize                     = DEFAULT_THREADPOOL_SIZE;
   private boolean                  sendMsgLeftLastSession             = DEFAULT_SEND_MSG_LEFT_LAST_SESSION;

   public ProducerMode getMode() {
      return mode;
   }

   /**
    * 设置消息发送模式，默认为同步(SYNC_MODE)
    * 
    * @param mode
    */
   public void setMode(ProducerMode mode) {
      this.mode = mode;
   }

   public int getRetryTimes() {
      return retryTimes;
   }

   /**
    * 设置消息发送的重试次数，默认为5
    * 
    * @param retryTimes
    */
   public void setRetryTimes(int retryTimes) {
      if (retryTimes < 0) {
         this.retryTimes = DEFAULT_RETRY_TIMES;
         logger.warn("invalid retryTimes, use default value: " + this.retryTimes + ".");
         return;
      }
      this.retryTimes = retryTimes;
   }

   public boolean isZipped() {
      return zipped;
   }

   /**
    * 设置是否压缩存储消息，默认为false<br>
    * Swallow将尝试进行压缩，如果压缩失败，则原文存储，不会作额外通知。
    * 
    * @param zipped
    */
   public void setZipped(boolean zipped) {
      this.zipped = zipped;
   }

   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * 设置异步模式(ASYNC_MODE)下发送线程池的线程数，默认为5
    * 
    * @param threadPoolSize
    */
   public void setThreadPoolSize(int threadPoolSize) {
      if (threadPoolSize <= 0 || threadPoolSize > MAX_THREADPOOL_SIZE) {
         this.threadPoolSize = DEFAULT_THREADPOOL_SIZE;
         logger.warn("invalid threadPoolSize, must between 1 - " + MAX_THREADPOOL_SIZE + ", use default value: "
               + this.threadPoolSize + ".");
         return;
      }
      this.threadPoolSize = threadPoolSize;
   }

   public boolean isSendMsgLeftLastSession() {
      return sendMsgLeftLastSession;
   }

   /**
    * 设置重启producer时是否发送上次未发送的消息，默认为false
    * 
    * @param sendMsgLeftLastSession
    */
   public void setSendMsgLeftLastSession(boolean sendMsgLeftLastSession) {
      this.sendMsgLeftLastSession = sendMsgLeftLastSession;
   }

   @Override
   public String toString() {
      return "Mode="
            + getMode()
            + "; RetryTimes="
            + getRetryTimes()
            + "; Zipped="
            + isZipped()
            + (getMode() == ProducerMode.ASYNC_MODE ? "; ThreadPoolSize=" + getThreadPoolSize()
                  + "; SendMsgLeftLastSession=" + isSendMsgLeftLastSession() : "");
   }
}
