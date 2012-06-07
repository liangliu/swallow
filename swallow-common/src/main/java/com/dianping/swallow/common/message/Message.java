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

   public static enum ContentType {
      /** 普通Bean对象 */
      BeanMessage(),
      /** 文本对象 */
      TextMessage(),
      /** 字节数组对象 */
      BytesMessage();
   }

}
