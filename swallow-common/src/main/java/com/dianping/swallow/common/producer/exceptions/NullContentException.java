package com.dianping.swallow.common.producer.exceptions;

/**
 * 调用Producer的SendMessage函数时，如果传入的Content为空，则抛出此异常
 * 
 * @author tong.song
 */
public class NullContentException extends RuntimeException {
   private static final long serialVersionUID = -6251131303134970375L;

   public NullContentException(String message, Throwable cause) {
      super(message, cause);
   }

   public NullContentException(Throwable cause) {
      super("Content can not be null.", cause);
   }
   
   public NullContentException(){
      super("Content can not be null.");
   }

}
