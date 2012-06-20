package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Properties;

public interface Message {

   Long getMessageId();

   Date getGeneratedTime();

   String getVersion();

   Properties getProperties();

   String getContent();

   String getSha1();

}
