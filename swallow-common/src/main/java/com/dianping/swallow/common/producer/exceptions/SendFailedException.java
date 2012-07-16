package com.dianping.swallow.common.producer.exceptions;

public class SendFailedException extends RuntimeException{
   private static final long serialVersionUID = -5152719369298952439L;
   
   private String info = "Message saved failed. Reason: ";
   
   public SendFailedException(String info){
      this.info += info;
   }
   
   @Override
   public String getMessage() {
      return info;
   }
}
