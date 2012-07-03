package com.dianping.swallow.common.producer.exceptions;

public class TopicNameInvalidException extends Exception {

   private static final long serialVersionUID = 5536528063495154789L;

   @Override
   public String getMessage() {
      return "Topic name is invalid, only ACCEPT Alphabets、Numbers and Underline.";
   }

}
