package com.dianping.swallow.consumerserver.buffer;

import java.util.List;

import com.dianping.swallow.common.consumer.MessageFilter;

public interface MessageRetriever {

   /**
    * 从数据库获取messageId大于tailMessageId的消息，fetchSize可配置<br>
    * <p>
    * 注意：如果返回值messages.size大于0，则第一个元素一定是maxMessageId，
    * maxMessageId是本次访问数据库所获得的批量消息的最大messageId
    * ，用该值来更新tailMessageId，这样下次调用retriveMessage就不会访问数据库里已经访问过的消息。
    * </p>
    * 
    * @param topicName
    * @param tailMessageId
    * @param messageFilter 可以为null，如果为null则忽略
    * @return
    * @throws Exception
    */
   @SuppressWarnings("rawtypes")
   List retriveMessage(String topicName, Long tailMessageId, MessageFilter messageFilter);

   void setFetchSize(int fetchSize);

}
