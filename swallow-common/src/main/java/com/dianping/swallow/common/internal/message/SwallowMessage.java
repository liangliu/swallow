package com.dianping.swallow.common.internal.message;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import com.dianping.swallow.common.internal.codec.JsonBinder;
import com.dianping.swallow.common.message.Message;

public class SwallowMessage implements Serializable, Message {

   private static final long   serialVersionUID = -7019466307875540596L;

   private Date                generatedTime;

   private Long                messageId;

   private Map<String, String> properties;

   private Map<String, String> internalProperties;

   private String              version;

   private String              content;

   private String              sha1;

   private String              type;

   private String              sourceIp;

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

   public Map<String, String> getInternalProperties() {
      return internalProperties;
   }

   public void setInternalProperties(Map<String, String> internalProperties) {
      this.internalProperties = internalProperties;
   }

   public String getSha1() {
      return sha1;
   }

   public void setSha1(String sha1) {
      this.sha1 = sha1;
   }

   public <T> T transferContentToBean(Class<T> clazz) {
      JsonBinder jsonBinder = JsonBinder.buildBinder();
      return jsonBinder.fromJson(content, clazz);
   }

   public String getContent() {
      return content;
   }

   public void setContent(Object content) {
      if (content instanceof String) {
         this.content = (String) content;
      } else {
         JsonBinder jsonBinder = JsonBinder.buildBinder();
         this.content = jsonBinder.toJson(content);
      }
   }

   public String getType() {
      return type;
   }

   public void setType(String type) {
      this.type = type;
   }

   public String getSourceIp() {
      return sourceIp;
   }

   public void setSourceIp(String sourceIp) {
      this.sourceIp = sourceIp;
   }

   @Override
   public String toString() {
      return String
            .format(
                  "SwallowMessage [generatedTime=%s, messageId=%s, properties=%s, version=%s, content=%s, sha1=%s, type=%s, sourceIp=%s]",
                  generatedTime, messageId, properties, version, content, sha1, type, sourceIp);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      SwallowMessage other = (SwallowMessage) obj;
      if (messageId == null) {
         if (other.messageId != null)
            return false;
      } else if (!messageId.equals(other.messageId))
         return false;
      return true;
   }

   /**
    * 在不比较MessageId的情况下，判断消息是否相等。
    */
   public boolean equalsWithoutMessageId(Object obj) {
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
      if (sourceIp == null) {
         if (other.sourceIp != null)
            return false;
      } else if (!sourceIp.equals(other.sourceIp))
         return false;
      if (type == null) {
         if (other.type != null)
            return false;
      } else if (!type.equals(other.type))
         return false;
      if (version == null) {
         if (other.version != null)
            return false;
      } else if (!version.equals(other.version))
         return false;
      return true;
   }

}
