package com.dianping.swallow.common.message;

public class TextMessage extends AbstractMessage<String> {

   private static final long serialVersionUID = 5442536754432626639L;

   private String            content;

   public TextMessage() {
      this.setContentType(Message.ContentType.TextMessage);
   }

   @Override
   public String getContent() {
      return content;
   }

   @Override
   public void setContent(String content) {
      this.content = content;
   }

}
