package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.dao.AckDAO;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class AckDAOImpl implements AckDAO {

   @SuppressWarnings("unused")
   private static final Logger LOG         = LoggerFactory.getLogger(AckDAOImpl.class);

   public static final String  MSG_ID      = "mid";
   public static final String  CONSUMER_ID = "cid";

   private MongoClient         mongoClient;

   public void setMongoClient(MongoClient mongoClient) {
      this.mongoClient = mongoClient;
   }

   @Override
   public Long getMaxMessageId(String topicName, String consumerId) {
      DBCollection collection = this.mongoClient.getAckCollection(topicName);

      DBObject query = BasicDBObjectBuilder.start().add(CONSUMER_ID, consumerId).get();
      DBObject fields = BasicDBObjectBuilder.start().add(MSG_ID, Integer.valueOf(1)).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add(MSG_ID, Integer.valueOf(-1)).get();
      DBCursor cursor = collection.find(query, fields).sort(orderBy).limit(1);
      try {
         if (cursor.hasNext()) {
            DBObject result = cursor.next();
            BSONTimestamp timestamp = (BSONTimestamp) result.get(MSG_ID);
            return MongoUtils.BSONTimestampToLong(timestamp);
         }
      } finally {
         cursor.close();
      }
      return null;
   }

   @Override
   public void add(String topicName, String consumerId, Long messageId) {
      DBCollection collection = this.mongoClient.getAckCollection(topicName);

      BSONTimestamp timestamp = MongoUtils.longToBSONTimestamp(messageId);
      DBObject add = BasicDBObjectBuilder.start().add(CONSUMER_ID, consumerId).add(MSG_ID, timestamp).get();
      collection.insert(add);
   }

}
