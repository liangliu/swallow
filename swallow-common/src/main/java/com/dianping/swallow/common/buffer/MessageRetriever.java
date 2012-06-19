package com.dianping.swallow.common.buffer;

import java.util.List;

import com.dianping.swallow.common.message.Message;

public interface MessageRetriever {

   /**
    * 从数据库获取时间戳大于tailMessageTimeStamp的消息，fetchSize可配置
    * 
    * @param topicName
    * @param cid
    * @param tailMessageTimeStamp
    * @return
    * @throws Exception
    */
   List<Message> retriveMessage(String topicName, Long tailMessageTimeStamp) throws Exception;

   void setFetchSize(int fetchSize);

}
