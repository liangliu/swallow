package com.dianping.swallow.producer;

public class ProducerConfig {

   private ProducerMode mode = ProducerMode.SYNC_MODE;
   private int retryTimes = 5;
   private boolean zipped = false;
   private int threadPoolSize = 5;
   private boolean sendMsgLeftLastSession = false;
   
   public ProducerMode getMode() {
      return mode;
   }
   public void setMode(ProducerMode mode) {
      this.mode = mode;
   }
   public int getRetryTimes() {
      return retryTimes;
   }
   public void setRetryTimes(int retryTimes) {
      this.retryTimes = retryTimes;
   }
   public boolean isZipped() {
      return zipped;
   }
   public void setZipped(boolean zipped) {
      this.zipped = zipped;
   }
   public int getThreadPoolSize() {
      return threadPoolSize;
   }
   public void setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
   }
   public boolean isSendMsgLeftLastSession() {
      return sendMsgLeftLastSession;
   }
   public void setSendMsgLeftLastSession(boolean sendMsgLeftLastSession) {
      this.sendMsgLeftLastSession = sendMsgLeftLastSession;
   }
   
}
