package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Map;

public interface Message {

   Long getMessageId();

   Date getGeneratedTime();

   String getVersion();

   Map<String, String> getProperties();

   String getContent();

   String getSha1();

}
