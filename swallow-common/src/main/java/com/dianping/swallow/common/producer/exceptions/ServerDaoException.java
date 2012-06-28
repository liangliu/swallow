package com.dianping.swallow.common.producer.exceptions;

public class ServerDaoException extends Exception {

   @Override
   public String getMessage() {
      return "Can not save message to DB now.";
   }

}
