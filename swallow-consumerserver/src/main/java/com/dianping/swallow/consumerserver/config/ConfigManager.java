package com.dianping.swallow.consumerserver.config;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhang.yu
 */
public final class ConfigManager {

   private static final Logger  LOG                             = LoggerFactory.getLogger(ConfigManager.class);

   private static ConfigManager ins                             = new ConfigManager();

   // time related
   private int                  pullFailDelayBase               = 500;
   private int                  pullFailDelayUpperBound         = 3000;
   private long                 checkConnectedChannelInterval   = 10000L;
   private long                 retryIntervalWhenMongoException = 2000L;
   private long                 waitAckTimeWhenCloseSwc         = 15000L;
   private long                 waitSlaveShutDown               = 30000L;
   private long                 closeChannelMaxWaitingTime      = 10000L;
   private int                  heartbeatCheckInterval          = 2000;
   private int                  heartbeatMaxStopTime            = 10000;
   private int                  heartbeatUpdateInterval         = 2000;
   private int                  maxClientThreadCount            = 100;
   private int                  masterPort                      = 8081;
   private int                  slavePort                       = 8082;
   private int                  maxAckedMessageIdUpdateInterval = 1000;

   //Master Ip
   private String               masterIp                        = "127.0.0.1";

   public int getPullFailDelayBase() {
      return pullFailDelayBase;
   }

   public int getMaxClientThreadCount() {
      return maxClientThreadCount;
   }

   public int getMasterPort() {
      return masterPort;
   }

   public int getSlavePort() {
      return slavePort;
   }

   public long getCloseChannelMaxWaitingTime() {
      return closeChannelMaxWaitingTime;
   }

   public int getPullFailDelayUpperBound() {
      return pullFailDelayUpperBound;
   }

   public long getCheckConnectedChannelInterval() {
      return checkConnectedChannelInterval;
   }

   public long getRetryIntervalWhenMongoException() {
      return retryIntervalWhenMongoException;
   }

   public long getWaitAckTimeWhenCloseSwc() {
      return waitAckTimeWhenCloseSwc;
   }

   public long getWaitSlaveShutDown() {
      return waitSlaveShutDown;
   }

   public String getMasterIp() {
      return masterIp;
   }

   public int getHeartbeatCheckInterval() {
      return heartbeatCheckInterval;
   }

   /***
    * @return master consumer心跳最长的停止时间
    */
   public int getHeartbeatMaxStopTime() {
      return heartbeatMaxStopTime;
   }

   /***
    * @return master consumer更新心跳的间隔
    */
   public int getHeartbeatUpdateInterval() {
      return heartbeatUpdateInterval;
   }

   public int getMaxAckedMessageIdUpdateInterval() {
      return maxAckedMessageIdUpdateInterval;
   }

   public static void main(String[] args) {
      new ConfigManager();
   }

   public static ConfigManager getInstance() {
      return ins;
   }

   private ConfigManager() {
      this("swallow-consumerserver.properties");
   }

   @SuppressWarnings("rawtypes")
   private ConfigManager(String configFileName) {
      InputStream in = ConfigManager.class.getClassLoader().getResourceAsStream(configFileName);
      Properties props = new Properties();
      Class clazz = this.getClass();
      if (in != null) {
         try {
            props.load(in);
            in.close();
            for (String key : props.stringPropertyNames()) {
               Field field = null;
               try {
                  field = clazz.getDeclaredField(key.trim());
               } catch (Exception e) {
                  LOG.error("unknow property found in " + configFileName + ": " + key);
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
                     LOG.error("can not set property " + key, e);
                     continue;
                  }
               }
            }

         } catch (IOException e) {
            LOG.error("Error reading " + configFileName, e);
         }
      } else {
         LOG.info(configFileName + " not found, use default");
      }
      //设置masterIP
      String masterIp = System.getProperty("masterIp");
      if (masterIp != null && masterIp.length() > 0) {
         this.masterIp = masterIp;
      }
      //打印参数
      Field[] fields = clazz.getDeclaredFields();
      for (int i = 0; i < fields.length; i++) {
         Field f = fields[i];
         f.setAccessible(true);
         if (!Modifier.isStatic(f.getModifiers())) {
            try {
               LOG.info(f.getName() + "=" + f.get(this));
            } catch (Exception e) {
            }
         }
      }
   }
}
