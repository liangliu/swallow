package com.dianping.swallow.consumer;

/**
 * 当consumer无法处理Message且希望重试该消息时，可以抛出该异常，Swallow接收到该异常会使用重试策略（见
 * {@link com.dianping.swallow.consumer.ConsumerConfig} 的
 * delayBaseOnBackoutMessageException ，delayUpperboundOnBackoutMessageException
 * ，retryCountOnBackoutMessageException选项）进行重试
 * 
 * @author kezhu.wu
 */
public class BackoutMessageException extends Exception {

   private static final long serialVersionUID = 3860244803030243875L;

   public BackoutMessageException() {
      super();
   }

   public BackoutMessageException(String message, Throwable cause) {
      super(message, cause);
   }

   public BackoutMessageException(String message) {
      super(message);
   }

   public BackoutMessageException(Throwable cause) {
      super(cause);
   }

}
