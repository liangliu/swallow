package com.dianping.swallow.common.producer.exceptions;

public class RemoteServiceDownException extends RuntimeException{
   private static final long serialVersionUID = -8885826779834945921L;

   @Override
   public String getMessage() {
      return "Remote service's status is unusual.";
   }
}
