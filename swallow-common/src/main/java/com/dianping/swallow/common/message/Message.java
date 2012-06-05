package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Properties;

public interface Message<T> {

   Long getMessageId();

   void setMessageId(Long messageId);

   ContentType getContentType();

   void setContentType(ContentType contentType);

   Date getGeneratedTime();

   void setGeneratedTime(Date generatedTime);

   int getRetryCount();

   void setRetryCount(int retryCount);

   String getVersion();

   void setVersion(String version);

   Properties getProperties();

   void setProperties(Properties properties);

   T getContent();

   void setContent(T content);

   public enum ContentType {
      /** 普通Java对象 */
      ObjectMessage(),
      /** Json对象 */
      JsonMessage(),
      /** 文本对象 */
      TextMessage(),
      /** 字节数组对象 */
      BytesMessage();
   }

}
