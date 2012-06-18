package com.dianping.swallow.common.message2;

import java.io.Serializable;
import java.util.Date;
import java.util.Properties;

import com.dianping.swallow.common.dao.impl.mongodb.MessageId;
import com.dianping.swallow.common.message.JsonBinder;

public class SwallowMessage implements Serializable {

   private static final long serialVersionUID = -7019466307875540596L;

   private Date              generatedTime;

   private MessageId         messageId;

   private Properties        properties       = new Properties();

   private int               retryCount;

   private String            version;

   private String            content;

   private ContentType       contentType;

   private String            sha1;

   public Date getGeneratedTime() {
      return generatedTime;
   }

   public void setGeneratedTime(Date generatedTime) {
      this.generatedTime = generatedTime;
   }

   public MessageId getMessageId() {
      return messageId;
   }

   public void setMessageId(MessageId messageId) {
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

   public <T> T getContentAsBean(Class<T> clazz) {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      return jsonBinder.fromJson(content, clazz);
   }

   public String getContent() {
      return content;
   }

   public byte[] getContentAsBytes() {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      return jsonBinder.fromJson(content, byte[].class);
   }

   public void setContentAsJsonString(Object bean) {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      this.content = jsonBinder.toJson(bean);
   }

   public void setContentAsJsonString(byte[] content) {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      this.content = jsonBinder.toJson(content);
   }

   public void setContent(String content) {
      this.content = content;
   }

   public ContentType getContentType() {
      return contentType;
   }

   protected void setContentType(ContentType contentType) {
      this.contentType = contentType;
   }

   public static enum ContentType {
      /** 普通Bean对象 */
      BeanMessage(),
      /** 文本对象 */
      TextMessage(),
      /** 字节数组对象 */
      BytesMessage();
   }

   @Override
   public String toString() {
      return "SwallowMessage [generatedTime=" + generatedTime + ", messageId=" + messageId + ", properties="
            + properties + ", retryCount=" + retryCount + ", version=" + version + ", contentType=" + contentType
            + ", sha1=" + sha1 + ", content=" + content + "]";
   }

}
