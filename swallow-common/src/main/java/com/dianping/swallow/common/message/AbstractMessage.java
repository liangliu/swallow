package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Properties;

public abstract class AbstractMessage<T> implements Message<T> {

   private Date generatedTime;

   private Long messageId;

   private Properties properties = new Properties();

   private int retryCount;

   private String version;

   private ContentType contentType;

   public Date getGeneratedTime() {
      return generatedTime;
   }

   public void setGeneratedTime(Date generatedTime) {
      this.generatedTime = generatedTime;
   }

   public Long getMessageId() {
      return messageId;
   }

   public void setMessageId(Long messageId) {
      this.messageId = messageId;
   }

   public int getRetryCount() {
      return retryCount;
   }

   public void setRetryCount(int retryCount) {
      this.retryCount = retryCount;
   }

   public String getVersion() {
      return version;
   }

   public void setVersion(String version) {
      this.version = version;
   }

   public ContentType getContentType() {
      return contentType;
   }

   public void setContentType(ContentType contentType) {
      this.contentType = contentType;
   }

   public Properties getProperties() {
      return properties;
   }

   public void setProperties(Properties properties) {
      this.properties = properties;
   }

}
