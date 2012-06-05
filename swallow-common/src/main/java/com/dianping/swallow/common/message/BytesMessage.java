package com.dianping.swallow.common.message;

public class BytesMessage extends AbstractMessage<byte[]> {

   private byte[] content;

   @Override
   public byte[] getContent() {
      return content;
   }

   public void setContent(byte[] content) {
      this.content = content;
   }

}
