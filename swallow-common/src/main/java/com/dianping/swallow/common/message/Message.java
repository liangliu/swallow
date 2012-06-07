package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Properties;

public interface Message<T> {

   Long getMessageId();

   ContentType getContentType();

   Date getGeneratedTime();

   int getRetryCount();

   String getVersion();

   Properties getProperties();

   T getContent();

   public enum ContentType {
      /** 普通Java对象 */
      BeanMessage(),
      /** Json对象 */
      JsonMessage(),
      /** 文本对象 */
      TextMessage(),
      /** 字节数组对象 */
      BytesMessage();
   }

}
