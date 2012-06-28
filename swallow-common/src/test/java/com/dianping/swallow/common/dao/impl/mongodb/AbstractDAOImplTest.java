package com.dianping.swallow.common.dao.impl.mongodb;

import org.junit.After;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext.xml")
public abstract class AbstractDAOImplTest extends AbstractJUnit4SpringContextTests {

   protected static final String TOPIC_NAME = "topicForUnitTest";
   protected static final String IP         = "127.0.0.1";

   @Autowired
   private MongoClient           mongoClient;

   //   private DB                    db;

   //   @Before
   //   public void setUp() throws Exception {
   //      //如果db不存在，则创建db
   //      Mongo mongo = mongoClient.getMongo(TOPIC_NAME);
   //      db = mongo.getDB(this.getDBName());
   //   }

   @After
   public void tearDown() throws Exception {
      //删除测试过程创建的Collection
      mongoClient.getMessageCollection(TOPIC_NAME).drop();
      mongoClient.getAckCollection(TOPIC_NAME).drop();
      mongoClient.getHeartbeatCollection(IP.replace('.', '_')).drop();
   }

}
