package com.dianping.swallow.common.message;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class SwallowMessage implements Serializable, Message {

   private static final long   serialVersionUID = -7019466307875540596L;

   private Date                generatedTime;

   private Long                messageId;

   private Map<String, String> properties;

   private String              version;

   private String              content;

   private String              sha1;

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

   public String getVersion() {
      return version;
   }

   public void setVersion(String version) {
      this.version = version;
   }

   public Map<String, String> getProperties() {
      return properties;
   }

   public void setProperties(Map<String, String> properties) {
      this.properties = properties;
   }

   public String getSha1() {
      return sha1;
   }

   public void setSha1(String sha1) {
      this.sha1 = sha1;
   }

   public <T> T transferContentToBean(Class<T> clazz) {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      return jsonBinder.fromJson(content, clazz);
   }

   public String getContent() {
      return content;
   }

   public void setContent(Object content) {
      if (content instanceof String) {
         this.content = (String) content;
      } else {
         JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
         this.content = jsonBinder.toJson(content);
      }

   }

   @Override
   public String toString() {
      return "SwallowMessage [generatedTime=" + generatedTime + ", messageId=" + messageId + ", properties="
            + properties + ", version=" + version + ", sha1=" + sha1 + ", content=" + content + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((content == null) ? 0 : content.hashCode());
      result = prime * result + ((generatedTime == null) ? 0 : generatedTime.hashCode());
      result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
      result = prime * result + ((properties == null) ? 0 : properties.hashCode());
      result = prime * result + ((sha1 == null) ? 0 : sha1.hashCode());
      result = prime * result + ((version == null) ? 0 : version.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof SwallowMessage))
         return false;
      SwallowMessage other = (SwallowMessage) obj;
      if (content == null) {
         if (other.content != null)
            return false;
      } else if (!content.equals(other.content))
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
      if (sha1 == null) {
         if (other.sha1 != null)
            return false;
      } else if (!sha1.equals(other.sha1))
         return false;
      if (version == null) {
         if (other.version != null)
            return false;
      } else if (!version.equals(other.version))
         return false;
      return true;
   }

}
