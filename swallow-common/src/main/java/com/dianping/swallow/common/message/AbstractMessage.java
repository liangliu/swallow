package com.dianping.swallow.common.message;

import java.io.Serializable;
import java.util.Date;
import java.util.Properties;

public abstract class AbstractMessage<T> implements Message<T>, Serializable {

   private static final long serialVersionUID = -7019466307875540596L;

   private Date              generatedTime;

   private Long              messageId;

   private Properties        properties       = new Properties();

   private int               retryCount;

   private String            version;

   private ContentType       contentType;

   private String            sha1;

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

   protected void setContentType(ContentType contentType) {
      this.contentType = contentType;
   }

   public Properties getProperties() {
      return properties;
   }

   public void setProperties(Properties properties) {
      this.properties = properties;
   }

   public String getSha1() {
      return sha1;
   }

   public void setSha1(String sha1) {
      this.sha1 = sha1;
   }

   public abstract void setContent(T content);

   @Override
   public String toString() {
      return String
            .format(
                  "%s [generatedTime=%s, messageId=%s, properties=%s, retryCount=%s, version=%s, contentType=%s, content=%s]",
                  this.getClass().getName(), generatedTime, messageId, properties, retryCount, version, contentType,
                  getContent());
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((contentType == null) ? 0 : contentType.hashCode());
      result = prime * result + ((generatedTime == null) ? 0 : generatedTime.hashCode());
      result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
      result = prime * result + ((properties == null) ? 0 : properties.hashCode());
      result = prime * result + retryCount;
      result = prime * result + ((version == null) ? 0 : version.hashCode());
      result = prime * result + ((getContent() == null) ? 0 : getContent().hashCode());
      return result;
   }

   @SuppressWarnings("rawtypes")
   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof AbstractMessage))
         return false;
      AbstractMessage other = (AbstractMessage) obj;
      if (contentType != other.contentType)
         return false;
      if (generatedTime == null) {
         if (other.generatedTime != null)
            return false;
      } else if (!generatedTime.equals(other.generatedTime))
         return false;
      if (messageId == null) {
         if (other.messageId != null)
            return false;
      } else if (!messageId.equals(other.messageId))
         return false;
      if (properties == null) {
         if (other.properties != null)
            return false;
      } else if (!properties.equals(other.properties))
         return false;
      if (retryCount != other.retryCount)
         return false;
      if (version == null) {
         if (other.version != null)
            return false;
      } else if (!version.equals(other.version))
         return false;
      if (getContent() == null) {
         if (other.getContent() != null)
            return false;
      } else if (!getContent().equals(other.getContent()))
         return false;
      return true;
   }

}
