package com.dianping.swallow.common.producer.exceptions;

public class SendFailedException extends Exception {
   private static final long serialVersionUID = -5152719369298952439L;

   public SendFailedException(String message, Throwable cause) {
      super(message, cause);
   }

   public SendFailedException(Throwable cause) {
      super(cause);
   }

}
