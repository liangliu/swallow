package com.dianping.swallow.consumerserver.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dianping.swallow.common.internal.dao.HeartbeatDAO;
import com.dianping.swallow.consumerserver.Heartbeater;

public class MongoHeartbeaterTest {

   @Test
   public void testWaitUntilMasterDown() throws Exception {
      HeartbeatDAO hbDao = mock(HeartbeatDAO.class);
      when(hbDao.findLastHeartbeat(Mockito.anyString())).then(new Answer<Date>() {

         Date d =new Date();
         
         @Override
         public Date answer(InvocationOnMock invocation) throws Throwable {
            return d;
         }
      });
      
      
      final AtomicBoolean waked = new AtomicBoolean(false);
      final Heartbeater hb = new MongoHeartbeater();

      //ProxyUtil.createMongoDaoProxyWithRetryMechanism不能和mock一起工作
      Field daoField = hb.getClass().getDeclaredField("heartbeatDAO");
      daoField.setAccessible(true);
      daoField.set(hb, hbDao);
      
      Thread t = new Thread() {
         public void run() {
            try {
               hb.waitUntilMasterDown("127.0.0.1", 100, 300);
               waked.set(true);
            } catch (InterruptedException e) {
            }
         }
      };
      t.start();
      t.join(400);
      Assert.assertTrue(waked.get());
   }
   
   @Test
   public void testwaitUntilMasterUp() throws Exception {
      HeartbeatDAO hbDao = mock(HeartbeatDAO.class);
      when(hbDao.findLastHeartbeat(Mockito.anyString())).then(new Answer<Date>() {

         Date d =new Date((new Date()).getTime() - 300);
         int reqTimes = 0;
         
         @Override
         public Date answer(InvocationOnMock invocation) throws Throwable {
            if(reqTimes++ > 4) {
               return new Date();
            }
            return d;
         }
      });
      
      
      final AtomicBoolean waked = new AtomicBoolean(false);
      final Heartbeater hb = new MongoHeartbeater();

      //ProxyUtil.createMongoDaoProxyWithRetryMechanism不能和mock一起工作
      Field daoField = hb.getClass().getDeclaredField("heartbeatDAO");
      daoField.setAccessible(true);
      daoField.set(hb, hbDao);
      
      Thread t = new Thread() {
         public void run() {
            try {
               hb.waitUntilMasterUp("127.0.0.1", 100, 300);
               waked.set(true);
            } catch (InterruptedException e) {
            }
         }
      };
      long beatStart = System.currentTimeMillis();
      t.start();
      t.join(600);
      Assert.assertTrue(System.currentTimeMillis() - beatStart > 300);
      Assert.assertTrue(waked.get());
   }
   
}
