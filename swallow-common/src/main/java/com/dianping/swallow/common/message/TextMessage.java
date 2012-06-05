package com.dianping.swallow.common.message;

public class TextMessage extends AbstractMessage<String> {

   private String content;

   @Override
   public String getContent() {
      return content;
   }

   public void setContent(String content) {
      this.content = content;
   }

}
