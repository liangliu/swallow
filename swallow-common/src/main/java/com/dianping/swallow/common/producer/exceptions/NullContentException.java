package com.dianping.swallow.common.producer.exceptions;

public class NullContentException extends Exception{
   private static final long serialVersionUID = -6251131303134970375L;

   @Override
   public String getMessage() {
      return "Content can not be null.";
   }

}
