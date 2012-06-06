package com.dianping.swallow.common.message;

public class TextMessage extends AbstractMessage<String> {

   private static final long serialVersionUID = 5442536754432626639L;

   private String            content;

   @Override
   public String getContent() {
      return content;
   }

   public void setContent(String content) {
      this.content = content;
   }

}
