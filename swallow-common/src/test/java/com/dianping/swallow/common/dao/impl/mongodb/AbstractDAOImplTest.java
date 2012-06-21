package com.dianping.swallow.common.dao.impl.mongodb;

import org.junit.After;
import org.junit.Before;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext.xml")
public abstract class AbstractDAOImplTest extends AbstractJUnit4SpringContextTests {

   protected static final String TOPIC_NAME = "topicForTest";

   private DBCollection          collection;

   @Autowired
   private MongoClient           mongoClient;

   @Before
   public void setUp() throws Exception {
      //如果db不存在，则创建db
      mongoClient.getConfig().getMessageDBName();
      Mongo mongo = mongoClient.getMongo(TOPIC_NAME);
      DB db = mongo.getDB(this.getDBName());
      //创建Collection
      if (db.collectionExists(TOPIC_NAME)) {
         db.getCollection(TOPIC_NAME).drop();
      }
      DBObject option = new BasicDBObject();
      option.put("capped", true);
      option.put("size", 512);
      collection = db.createCollection(TOPIC_NAME, option);
   }

   @After
   public void tearDown() throws Exception {
      //删除Collection
      collection.drop();
   }

   protected abstract String getDBName();

   public MongoConfig getMongoConfig() {
      return this.mongoClient.getConfig();
   }

}
