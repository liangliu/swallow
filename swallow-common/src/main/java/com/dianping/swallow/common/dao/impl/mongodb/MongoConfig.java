package com.dianping.swallow.common.dao.impl.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoConfig {

   private static final Logger LOG                                          = LoggerFactory
                                                                                  .getLogger(MongoConfig.class);
   // db collection name
   private String              messageDBName                                = "swallow_message";
   private String              ackDBName                                    = "swallow_ack";
   private String              heartbeatDBName                              = "swallow_heartbeat";

   // mongo server options
   private boolean             slaveOk                                      = false;
   private boolean             socketKeepAlive                              = false;
   private int                 socketTimeout                                = 500;
   private int                 connectionsPerHost                           = 30;
   private int                 threadsAllowedToBlockForConnectionMultiplier = 50;
   private int                 w                                            = 0;
   private int                 wtimeout                                     = 500;
   private boolean             fsync                                        = false;
   private int                 connectTimeout                               = 500;
   private int                 maxWaitTime                                  = 1000 * 60 * 2;
   private boolean             autoConnectRetry                             = false;
   private boolean             safe                                         = false;
   private int                 cappedCollectionSize                         = Integer.MAX_VALUE;
   private int                 cappedCollectionMaxDocNum;

   public MongoConfig() {
   }

   public MongoConfig(InputStream in) {
      init(in);
   }

   public MongoConfig(String configFileName) {
      InputStream in = MongoConfig.class.getClassLoader().getResourceAsStream(configFileName);
      init(in);
   }

   @SuppressWarnings("rawtypes")
   private void init(InputStream in) {
      Properties props = new Properties();
      try {
         props.load(in);
         in.close();
      } catch (IOException e1) {
         throw new RuntimeException(e1.getMessage(), e1);
      }

      Class clazz = this.getClass();
      for (String key : props.stringPropertyNames()) {
         Field field = null;
         try {
            field = clazz.getDeclaredField(key.trim());
         } catch (Exception e) {
            LOG.error("unknown property found: " + key);
            continue;
         }
         field.setAccessible(true);
         if (field.getType().equals(Integer.TYPE)) {
            try {
               field.set(this, Integer.parseInt(props.getProperty(key).trim()));
            } catch (Exception e) {
               LOG.error("can not parse property " + key, e);
               continue;
            }
         } else if (field.getType().equals(Long.TYPE)) {
            try {
               field.set(this, Long.parseLong(props.getProperty(key).trim()));
            } catch (Exception e) {
               LOG.error("can not set property " + key, e);
               continue;
            }
         } else if (field.getType().equals(String.class)) {
            try {
               field.set(this, props.getProperty(key).trim());
            } catch (Exception e) {
               LOG.error("can not set property " + key, e);
               continue;
            }
         } else {
            try {
               field.set(this, Boolean.parseBoolean(props.getProperty(key).trim()));
            } catch (Exception e) {
               LOG.error("cat not set property " + key, e);
               continue;
            }
         }
      }

      if (LOG.isDebugEnabled()) {
         Field[] fields = clazz.getDeclaredFields();
         for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            f.setAccessible(true);
            if (!Modifier.isStatic(f.getModifiers())) {
               try {
                  LOG.debug(f.getName() + "=" + f.get(this));
               } catch (Exception e) {
               }
            }
         }
      }
   }

   public String getMessageDBName() {
      return messageDBName;
   }

   public void setMessageDBName(String messageDBName) {
      this.messageDBName = messageDBName;
   }

   public String getAckDBName() {
      return ackDBName;
   }

   public void setAckDBName(String ackDBName) {
      this.ackDBName = ackDBName;
   }

   public String getHeartbeatDBName() {
      return heartbeatDBName;
   }

   public void setHeartbeatDBName(String heartbeatDBName) {
      this.heartbeatDBName = heartbeatDBName;
   }

   public boolean isSlaveOk() {
      return slaveOk;
   }

   public void setSlaveOk(boolean slaveOk) {
      this.slaveOk = slaveOk;
   }

   public boolean isSocketKeepAlive() {
      return socketKeepAlive;
   }

   public void setSocketKeepAlive(boolean socketKeepAlive) {
      this.socketKeepAlive = socketKeepAlive;
   }

   public int getSocketTimeout() {
      return socketTimeout;
   }

   public void setSocketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
   }

   public int getConnectionsPerHost() {
      return connectionsPerHost;
   }

   public void setConnectionsPerHost(int connectionsPerHost) {
      this.connectionsPerHost = connectionsPerHost;
   }

   public int getThreadsAllowedToBlockForConnectionMultiplier() {
      return threadsAllowedToBlockForConnectionMultiplier;
   }

   public void setThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
      this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
   }

   public int getW() {
      return w;
   }

   public void setW(int w) {
      this.w = w;
   }

   public int getWtimeout() {
      return wtimeout;
   }

   public void setWtimeout(int wtimeout) {
      this.wtimeout = wtimeout;
   }

   public boolean isFsync() {
      return fsync;
   }

   public void setFsync(boolean fsync) {
      this.fsync = fsync;
   }

   public int getConnectTimeout() {
      return connectTimeout;
   }

   public void setConnectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
   }

   public int getMaxWaitTime() {
      return maxWaitTime;
   }

   public void setMaxWaitTime(int maxWaitTime) {
      this.maxWaitTime = maxWaitTime;
   }

   public boolean isAutoConnectRetry() {
      return autoConnectRetry;
   }

   public void setAutoConnectRetry(boolean autoConnectRetry) {
      this.autoConnectRetry = autoConnectRetry;
   }

   public boolean isSafe() {
      return safe;
   }

   public void setSafe(boolean safe) {
      this.safe = safe;
   }

   public int getCappedCollectionSize() {
      return cappedCollectionSize;
   }

   public void setCappedCollectionSize(int cappedCollectionSize) {
      this.cappedCollectionSize = cappedCollectionSize;
   }

   public int getCappedCollectionMaxDocNum() {
      return cappedCollectionMaxDocNum;
   }

   public void setCappedCollectionMaxDocNum(int cappedCollectionMaxDocNum) {
      this.cappedCollectionMaxDocNum = cappedCollectionMaxDocNum;
   }

}
