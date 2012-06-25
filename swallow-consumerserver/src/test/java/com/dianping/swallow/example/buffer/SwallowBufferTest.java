package com.dianping.swallow.example.buffer;

import org.junit.After;
import org.junit.Test;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import com.dianping.swallow.consumerserver.buffer.SwallowBuffer;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext.xml")
public class SwallowBufferTest extends AbstractJUnit4SpringContextTests {

   protected static final String TOPIC_NAME = "topicForUnitTest";

   @Autowired
   private SwallowBuffer         swallowBuffer;

   //   private DB                    db;

   //   @Before
   //   public void setUp() throws Exception {
   //      //如果db不存在，则创建db
   //      Mongo mongo = mongoClient.getMongo(TOPIC_NAME);
   //      db = mongo.getDB(this.getDBName());
   //   }
   @Test
   public void a() {

   }

}
