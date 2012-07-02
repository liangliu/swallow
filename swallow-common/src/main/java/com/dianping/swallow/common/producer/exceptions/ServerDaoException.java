package com.dianping.swallow.common.producer.exceptions;

public class ServerDaoException extends Exception {

   private static final long serialVersionUID = -35938089521851134L;

   @Override
   public String getMessage() {
      return "Can not save message to DB now.";
   }

}
