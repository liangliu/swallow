package com.dianping.swallow.common.producer.exceptions;

public class RemoteServiceDownException extends Exception{
   @Override
   public String getMessage() {
      return "Remote service's status is unusual.";
   }
}
