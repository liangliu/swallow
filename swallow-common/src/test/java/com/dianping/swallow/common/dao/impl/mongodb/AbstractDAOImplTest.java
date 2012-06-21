package com.dianping.swallow.common.dao.impl.mongodb;

import org.junit.After;
import org.junit.Before;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.mongodb.DB;
import com.mongodb.Mongo;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext.xml")
public abstract class AbstractDAOImplTest extends AbstractJUnit4SpringContextTests {

   protected static final String TOPIC_NAME = "topicForTest";

   @Autowired
   private MongoClient           mongoClient;

   private DB                    db;

   @Before
   public void setUp() throws Exception {
      //如果db不存在，则创建db
      Mongo mongo = mongoClient.getMongo(TOPIC_NAME);
      db = mongo.getDB(this.getDBName());
   }

   @After
   public void tearDown() throws Exception {
      //删除测试过程创建的Collection
      if (db.collectionExists(TOPIC_NAME)) {
         db.getCollection(TOPIC_NAME).drop();
      }
   }

   protected abstract String getDBName();

   public MongoConfig getMongoConfig() {
      return this.mongoClient.getConfig();
   }

}
