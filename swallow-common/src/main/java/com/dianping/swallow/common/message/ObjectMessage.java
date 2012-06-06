package com.dianping.swallow.common.message;

import java.io.Serializable;

public class ObjectMessage extends AbstractMessage<Object> {

   private static final long serialVersionUID = 2465173281205737612L;

   private Object            content;

   @Override
   public Object getContent() {
      return content;
   }
   /**
    * @throws IllegalArgumentException if the 'content' argument did not implement the java.io.Serializable interface.
    */
   @Override
   public void setContent(Object content) {
      if(!(content instanceof Serializable)){
         throw new IllegalArgumentException("The 'content' argument must implement the java.io.Serializable interface.");
      }
      this.content = content;
   }

}
