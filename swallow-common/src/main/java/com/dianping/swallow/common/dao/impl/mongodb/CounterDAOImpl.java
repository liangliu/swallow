package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class CounterDAOImpl implements CounterDAO<Long> {

   @SuppressWarnings("unused")
   private static final Logger LOG = LoggerFactory.getLogger(CounterDAOImpl.class);

   private final DB            db;

   public CounterDAOImpl(DB db) {
      this.db = db;
   }

   @Override
   public Long getMaxMessageId(String topicName, String consumerId) {
      DBCollection collection = this.db.getCollection(topicName);
      DBObject query = BasicDBObjectBuilder.start().add("consumerId", consumerId).get();
      DBObject fields = BasicDBObjectBuilder.start().add("messageId", Integer.valueOf(1)).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("messageId", Integer.valueOf(-1)).get();
      DBCursor cursor = collection.find(query, fields).sort(orderBy).limit(1);
      DBObject result = cursor.next();
      BSONTimestamp timestamp = (BSONTimestamp) result.get("messageId");
      return BSONTimestampUtils.BSONTimestampToLong(timestamp);

   }

   @Override
   public void add(String topicName, String consumerId, Long messageId) {
      DBCollection collection = this.db.getCollection(topicName);
      BSONTimestamp timestamp = BSONTimestampUtils.longToBSONTimestamp(messageId);
      DBObject add = BasicDBObjectBuilder.start().add("consumerId", consumerId).add("messageId", timestamp).get();
      collection.insert(add, WriteConcern.SAFE);
   }

}
