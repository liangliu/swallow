package com.dianping.swallow.consumerserver.buffer;

import java.util.List;

import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.message.Message;

public interface MessageRetriever {

   /**
    * 从数据库获取messageId大于tailMessageId的消息，fetchSize可配置
    * 
    * @param topicName
    * @param tailMessageId
    * @param typeSet 可以为null，如果为null则忽略
    * @return
    * @throws Exception
    */
   List<Message> retriveMessage(String topicName, Long tailMessageId, MessageFilter messageFilter) throws Exception;

   void setFetchSize(int fetchSize);

}
