package com.dianping.swallow.consumerserver.buffer;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.SwallowMessage;

public class MongoDBMessageRetriever implements MessageRetriever {
   private static final Logger LOG       = LoggerFactory.getLogger(MongoDBMessageRetriever.class);

   private int                 fetchSize = 100;                                                   //默认100条

   private MessageDAO          messageDAO;

   public void setMessageDAO(MessageDAO messageDAO) {
      this.messageDAO = messageDAO;
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public List retriveMessage(String topicName, Long messageId, Set<String> messageTypeSet) throws Exception {
      List<SwallowMessage> messages;
      if (messageTypeSet != null && !messageTypeSet.isEmpty()) {
         messages = messageDAO.getMessagesGreaterThan(topicName, messageId, messageTypeSet, fetchSize);
      } else {
         messages = messageDAO.getMessagesGreaterThan(topicName, messageId, fetchSize);
      }

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
