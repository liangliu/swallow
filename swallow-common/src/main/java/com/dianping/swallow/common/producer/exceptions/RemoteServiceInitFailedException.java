package com.dianping.swallow.common.producer.exceptions;

public class RemoteServiceInitFailedException extends Exception {
   @Override
   public String getMessage() {
      return "Remote service initial failed.";
   }
}
