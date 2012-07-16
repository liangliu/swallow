package com.dianping.swallow.producer;

public class ProducerConfig {

   private ProducerMode mode                   = ProducerMode.SYNC_MODE;
   private int          retryTimes             = 5;
   private boolean      zipped                 = false;
   private int          threadPoolSize         = 5;
   private boolean      sendMsgLeftLastSession = false;

   public ProducerMode getMode() {
      return mode;
   }

   /**
    * 设置消息发送模式，默认为同步(SYNC_MODE)
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
    * @param retryTimes
    */
   public void setRetryTimes(int retryTimes) {
      this.retryTimes = retryTimes;
   }

   public boolean isZipped() {
      return zipped;
   }

   /**
    * 设置是否压缩存储消息，默认为false
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
    * @param threadPoolSize
    */
   public void setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
   }

   public boolean isSendMsgLeftLastSession() {
      return sendMsgLeftLastSession;
   }

   /**
    * 设置重启producer时是否发送上次未发送的消息，默认为false
    * @param sendMsgLeftLastSession
    */
   public void setSendMsgLeftLastSession(boolean sendMsgLeftLastSession) {
      this.sendMsgLeftLastSession = sendMsgLeftLastSession;
   }

}
