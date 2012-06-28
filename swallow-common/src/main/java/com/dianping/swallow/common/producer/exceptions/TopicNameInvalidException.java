package com.dianping.swallow.common.producer.exceptions;

public class TopicNameInvalidException extends Exception {

   @Override
   public String getMessage() {
      return "Topic name is invalid.";
   }

}
