package com.dianping.swallow.common.buffer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.SwallowMessage;

public class MongoDBMessageRetriever implements MessageRetriever {
   private static final Logger LOG       = LoggerFactory.getLogger(MongoDBMessageRetriever.class);

   private int                 fetchSize = 20;                                                    //默认20条

   private MessageDAO<Long>    messageDAO;

   public void setMessageDAO(MessageDAO<Long> messageDAO) {
      this.messageDAO = messageDAO;
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public List retriveMessage(String topicName, Long messageId) throws Exception {
      //mock db访问
      //      List<Message> list = new ArrayList<Message>();
      //      for (int i = 0; i < fetchSize; i++) {
      //         SwallowMessage message = new SwallowMessage();
      //         message.setMessageId(i + 1L);
      //         message.setContent("this is a SwallowMessage");
      //         message.setGeneratedTime(new Date());
      //         message.getProperties().setProperty("property-key", "property-value");
      //         message.setSha1("sha-1 string");
      //         message.setVersion("0.6.0");
      //         list.add(message);
      //         if (LOG.isDebugEnabled()) {
      //            LOG.debug("fetch message from mongodb:" + message.toString());
      //         }
      //         Thread.sleep(50L);//睡眠
      //      }
      //      Thread.sleep(100L);//睡眠
      //      return list;
      List<SwallowMessage> messages = messageDAO.getMessagesGreaterThan(topicName, messageId, fetchSize);
      if (LOG.isDebugEnabled()) {
         LOG.debug("fetched messages from mongodb, size:" + messages.size());
         LOG.debug("messages:" + messages);
      }
      return messages;
   }

   @Override
   public void setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
   }

}
