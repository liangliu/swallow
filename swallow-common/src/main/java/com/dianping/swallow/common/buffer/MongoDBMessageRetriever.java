package com.dianping.swallow.common.buffer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.TextMessage;

public class MongoDBMessageRetriever implements MessageRetriever {
   private static final Logger LOG       = LoggerFactory.getLogger(MongoDBMessageRetriever.class);

   private int                 fetchSize = 20;                                                    //默认20条

   @SuppressWarnings("rawtypes")
   @Override
   public List<Message> retriveMessage(String topicName, Long cid, Long tailMessageTimeStamp) throws Exception {
      //TODO mock db访问
      List<Message> list = new ArrayList<Message>();
      for (int i = 0; i < fetchSize; i++) {
         TextMessage m = new TextMessage();
         m.setContent("content:" + (i + 1));
         m.setMessageId(System.currentTimeMillis());
         list.add(m);
         if (LOG.isDebugEnabled()) {
            LOG.debug("fetch message from mongodb:" + m.toString());
         }
         Thread.sleep(50L);//睡眠
      }
      Thread.sleep(100L);//睡眠
      return list;
   }

   @Override
   public void setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
   }

}
