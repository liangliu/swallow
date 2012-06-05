package com.dianping.swallow.common.message;

public class ObjectMessage extends AbstractMessage<Object> {

   private Object content;

   @Override
   public Object getContent() {
      return content;
   }

   @Override
   public void setContent(Object content) {
      this.content = content;
   }

}
