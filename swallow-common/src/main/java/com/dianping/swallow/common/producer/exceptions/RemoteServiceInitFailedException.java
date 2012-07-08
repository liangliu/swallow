package com.dianping.swallow.common.producer.exceptions;

public class RemoteServiceInitFailedException extends RuntimeException {
   private static final long serialVersionUID = 8096198828080692568L;

   @Override
   public String getMessage() {
      return "Remote service initial failed.";
   }
}
