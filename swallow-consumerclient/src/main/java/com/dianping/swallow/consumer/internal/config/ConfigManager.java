package com.dianping.swallow.consumer.internal.config;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 该类用于设置Consumer的选项，所有Consumer共用一个ConsumerConfig。 <br>
 * <em>Internal-use-only</em> used by Swallow. <strong>DO NOT</strong> access
 * this class outside of Swallow.
 * 
 * @author zhang.yu
 */
public final class ConfigManager {

   private static final Logger  LOG                   = LoggerFactory.getLogger(ConfigManager.class);

   private static ConfigManager ins                   = new ConfigManager();
   // 连接master的间隔，下面是连接slave的间隔
   private long                 connectMasterInterval = 5000L;
   private long                 connectSlaveInterval  = 5000L;

   public long getConnectMasterInterval() {
      return connectMasterInterval;
   }

   public long getConnectSlaveInterval() {
      return connectSlaveInterval;
   }

   public static ConfigManager getInstance() {
      return ins;
   }

   private ConfigManager() {
      this("swallow-consumerclient.properties");
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
}
