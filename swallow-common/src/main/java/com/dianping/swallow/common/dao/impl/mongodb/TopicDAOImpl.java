package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.message2.SwallowMessage;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TopicDAOImpl implements TopicDAO<MessageId> {

   @SuppressWarnings("unused")
   private static final Logger LOG = LoggerFactory.getLogger(TopicDAOImpl.class);

   private final DB            db;

   public TopicDAOImpl(DB db) {
      this.db = db;
   }

   /**
    * 记录的格式如：<br>
    * { "_id" : ObjectId("4fdb03799414852ac300ae60"),<br>
    * "content" :"this is a message content.", <br>
    * "content-type" : "TextMessage",<br>
    * "generatedTime" : "2009-09-09 20:00:00", <br>
    * "properties" : "",<br>
    * "retryCount" : "1", <br>
    * "version" : "0.6.0", <br>
    * "sha1" : "" }
    */
   @Override
   public List<SwallowMessage> getMessagesGreaterThan(MessageId messageId, String topicName, int size) {
      DBCollection collection = this.db.getCollection(topicName);
      DBObject gt = BasicDBObjectBuilder.start().add("$gt", MessageId.toObjectId(messageId)).get();
      DBObject query = BasicDBObjectBuilder.start().add("_id", gt).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(1)).get();
      DBCursor cursor = collection.find(query).sort(orderBy).limit(size);
      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      while (cursor.hasNext()) {
         DBObject result = cursor.next();
         SwallowMessage swallowMessage = new SwallowMessage();
         ObjectId objectId = (ObjectId) result.get("_id");
         swallowMessage.setMessageId(MessageId.fromObjectId(objectId));
         swallowMessage.setContent((String) result.get("content"));
         swallowMessage.setVersion((String) result.get("version"));
         list.add(swallowMessage);
      }

      return list;
   }

   public static void main(String[] args) {
      String uri = "mongodb://localhost:27017";
      MongoClient mongoClient = new MongoClient(uri, new ConfigManager());
      TopicDAOImpl dao = new TopicDAOImpl(mongoClient.mongo.getDB("topic"));

      MessageId messageId = MessageId.fromObjectId(new ObjectId("4fdb0c399414852ac300ae64"));
      System.out.println(dao.getMessagesGreaterThan(messageId, "topicA", 3));

   }

}
