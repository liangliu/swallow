package com.dianping.swallow.consumerserver.buffer;

import java.util.List;

import com.dianping.swallow.common.message.Message;

public interface MessageRetriever {

   /**
    * 从数据库获取messageId大于tailMessageId的消息，fetchSize可配置
    * 
    * @param topicName
    * @param cid
    * @param tailMessageId
    * @return
    * @throws Exception
    */
   List<Message> retriveMessage(String topicName, Long tailMessageId) throws Exception;

   void setFetchSize(int fetchSize);

}
