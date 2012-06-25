package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.dao.HeartbeatDAO;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class HeartbeatDAOImpl implements HeartbeatDAO {
   @SuppressWarnings("unused")
   private static final Logger LOG = LoggerFactory.getLogger(HeartbeatDAOImpl.class);

   private MongoClient         mongoClient;

   public void setMongoClient(MongoClient mongoClient) {
      this.mongoClient = mongoClient;
   }

   @Override
   public Date updateLastHeartbeat(String ip) {
      DBCollection collection = this.mongoClient.getHeartbeatCollection(ip.replace('.', '_'));

      Date curTime = new Date();
      DBObject insert = BasicDBObjectBuilder.start().add("t", curTime).get();
      collection.insert(insert);
      return curTime;
   }

   @Override
   public Date findLastHeartbeat(String ip) {
      DBCollection collection = this.mongoClient.getHeartbeatCollection(ip.replace('.', '_'));

      DBObject fields = BasicDBObjectBuilder.start().add("t", Integer.valueOf(1)).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("t", Integer.valueOf(-1)).get();
      DBCursor cursor = collection.find(null, fields).sort(orderBy).limit(1);
      while (cursor != null) {
         DBObject result = cursor.next();
         return (Date) result.get("t");
      }
      return null;
   }

}
