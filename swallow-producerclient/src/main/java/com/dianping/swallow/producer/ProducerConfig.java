package com.dianping.swallow.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer配置，默认构造函数生成的配置为：<br />
 * <table>
 * <tr>
 * <td>mode</td>
 * <td>=</td>
 * <td>{@link ProducerMode}.SYNC_MODE</td>
 * </tr>
 * <tr>
 * <td>retryTimes</td>
 * <td>=</td>
 * <td>5</td>
 * </tr>
 * <tr>
 * <td>zipped</td>
 * <td>=</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>threadPoolSize</td>
 * <td>=</td>
 * <td>5</td>
 * </tr>
 * <tr>
 * <td>sendMsgLeftLastSession</td>
 * <td>=</td>
 * <td>false</td>
 * </tr>
 * </table>
 * 
 * @author tong.song
 */
public class ProducerConfig {
   private static final Logger      LOGGER                             = LoggerFactory.getLogger(ProducerConfig.class);

   public static final int          MAX_THREADPOOL_SIZE                = 100;                                          //线程池大小最大值
   public static final ProducerMode DEFAULT_PRODUCER_MODE              = ProducerMode.SYNC_MODE;                       //默认Producer工作模式
   public static final int          DEFAULT_RETRY_TIMES                = 5;                                            //默认发送失败重试次数
   public static final boolean      DEFAULT_ZIPPED                     = false;                                        //默认是否对消息进行压缩
   public static final int          DEFAULT_THREADPOOL_SIZE            = 5;                                            //默认线程池大小
   public static final boolean      DEFAULT_SEND_MSG_LEFT_LAST_SESSION = false;                                        //默认是否重启续传

   private ProducerMode             mode                               = DEFAULT_PRODUCER_MODE;                        //Producer工作模式
   private int                      retryTimes                         = DEFAULT_RETRY_TIMES;                          //发送失败重试次数
   private boolean                  zipped                             = DEFAULT_ZIPPED;                               //是否对待发送消息进行压缩
   private int                      threadPoolSize                     = DEFAULT_THREADPOOL_SIZE;                      //异步模式时，线程池大小
   private boolean                  sendMsgLeftLastSession             = DEFAULT_SEND_MSG_LEFT_LAST_SESSION;           //异步模式时，是否重启续传

   /**
    * @return Producer工作模式，类型为{@link ProducerMode}
    */
   public ProducerMode getMode() {
      return mode;
   }

   /**
    * 设置消息发送模式，默认为同步{@link ProducerMode}<code>.SYNC_MODE</code>
    * 
    * @param mode Producer工作模式
    */
   public void setMode(ProducerMode mode) {
      this.mode = mode;
   }

   /**
    * @return 发送失败重试次数
    */
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
         LOGGER.warn("invalid retryTimes, use default value: " + this.retryTimes + ".");
         return;
      }
      this.retryTimes = retryTimes;
   }

   /**
    * @return 是否对待发送消息进行压缩
    */
   public boolean isZipped() {
      return zipped;
   }

   /**
    * 设置是否压缩存储消息，默认为false<br>
    * Swallow将尝试进行压缩，如果压缩失败，则原文存储，不会作额外通知
    * 
    * @param zipped 是否压缩
    */
   public void setZipped(boolean zipped) {
      this.zipped = zipped;
   }

   /**
    * @return 异步模式下线程池大小
    */
   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * 设置异步模式（{@link ProducerMode}<code>.ASYNC_MODE</code>
    * ）从FileQueue获取并发送消息的线程数量，默认为5
    * 
    * @param threadPoolSize 线程池大小
    */
   public void setThreadPoolSize(int threadPoolSize) {
      if (threadPoolSize <= 0 || threadPoolSize > MAX_THREADPOOL_SIZE) {
         this.threadPoolSize = DEFAULT_THREADPOOL_SIZE;
         LOGGER.warn("invalid threadPoolSize, must between 1 - " + MAX_THREADPOOL_SIZE + ", use default value: "
               + this.threadPoolSize + ".");
         return;
      }
      this.threadPoolSize = threadPoolSize;
   }

   /**
    * @return 异步模式时，重启Producer是否继续上次未完成的发送
    */
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
