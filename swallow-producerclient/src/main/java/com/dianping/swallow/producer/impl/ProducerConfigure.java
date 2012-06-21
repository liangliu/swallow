package com.dianping.swallow.producer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.dianping.swallow.common.producer.Destination;
import com.dianping.swallow.producer.ProducerMode;

public final class ProducerConfigure {

   private static final Logger logger               = Logger.getLogger(ProducerConfigure.class);

   private ProducerMode        producerMode;
   private Destination         destination;

   //以下为配置文件内容
   private String              producerModeStr      = "sync";
   private String              destinationName      = "master.slave";
   private int                 threadPoolSize       = 10;
   private int                 remoteServiceTimeout = 5000;
   private boolean             continueSend         = false;

   /**
    * 从producerModeStr及destinationName初始化producerMode及destination
    */
   private void initParams() {
      setProducerMode((producerModeStr == "async") ? ProducerMode.ASYNC_MODE : ProducerMode.SYNC_MODE);
      setDestination(Destination.topic(destinationName));
   }

   /**
    * 加载默认producer配置
    */
   public ProducerConfigure() {
      initParams();
   }

   /**
    * 根据配置文件加载producer配置
    * @param configFile producer配置文件目录及文件名
    */
   @SuppressWarnings("rawtypes")
   public ProducerConfigure(String configFile) {
      Properties props = new Properties();
      Class clazz = this.getClass();
      InputStream in = null;
      in = ProducerConfigure.class.getClassLoader().getResourceAsStream(configFile);

      try {
         props.load(in);
      } catch (IOException e) {
         logger.log(Level.ERROR, "Load property file failed.", e.getCause());
      } finally {
         if (in != null) {
            try {
               in.close();
            } catch (IOException e) {
               logger.log(Level.ERROR, "Close inputstream failed", e.getCause());
            }
         }
      }

      for (String key : props.stringPropertyNames()) {
         Field field = null;
         try {
            field = clazz.getDeclaredField(key.trim());
         } catch (Exception e) {
            logger.log(Level.WARN, "Unknow property found in" + configFile + ": " + key + ".", e.getCause());
            continue;
         }
         field.setAccessible(true);

         if (field.getType().equals(Integer.TYPE)) {
            try {
               field.set(this, Integer.parseInt(props.getProperty(key).trim()));
            } catch (Exception e) {
               logger.log(Level.ERROR, "Can not parse property" + key, e.getCause());
               continue;
            }
         } else if (field.getType().equals(Boolean.TYPE)) {
            try {
               field.set(this, Boolean.parseBoolean(props.getProperty(key).trim()));
            } catch (Exception e) {
               logger.log(Level.ERROR, "Can not parse property" + key, e.getCause());
               continue;
            }
         } else if (field.getType().equals(String.class)) {
            try {
               field.set(this, props.getProperty(key).trim());
            } catch (Exception e) {
               logger.log(Level.ERROR, "Can not set property" + key, e.getCause());
               continue;
            }
         }
      }
      if (logger.isDebugEnabled()) {
         Field[] fields = clazz.getDeclaredFields();
         for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            f.setAccessible(true);
            if (!Modifier.isStatic(f.getModifiers())) {
               try {
                  logger.debug(f.getName() + "=" + f.get(this));
               } catch (Exception e) {
               }
            }
         }
      }

      initParams();
   }

   /**
    * @return producer工作模式
    */
   public ProducerMode getProducerMode() {
      return producerMode;
   }

   /**
    * @param producerMode
    *           设置producer工作模式（同步：ProducerMode.SYNC，异步：ProducerMode.ASYNC）
    */
   public void setProducerMode(ProducerMode producerMode) {
      this.producerMode = producerMode;
   }

   /**
    * @return producer发送消息的目的地
    */
   public Destination getDestination() {
      return destination;
   }

   /**
    * @param destination producer发送消息的目的地（Destination.topic(String
    *           destinationName)）
    */
   public void setDestination(Destination destination) {
      this.destination = destination;
   }

   /**
    * @return 异步模式中线程池的大小
    */
   public int getThreadPoolSize() {
      return threadPoolSize;
   }

   /**
    * @param threadPoolSize =1时，APP内同一线程可保证有序，但发送速度较慢，>1时不保证有序
    */
   public void setThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = threadPoolSize;
   }

   /**
    * @return 远程调用的超时
    */
   public int getRemoteServiceTimeout() {
      return remoteServiceTimeout;
   }

   /**
    * @param remoteServiceTimeout 远程调用的超时
    */
   public void setRemoteServiceTimeout(int remoteServiceTimeout) {
      this.remoteServiceTimeout = remoteServiceTimeout;
   }

   /**
    * @return 是否重启续传，默认不续传
    */
   public boolean isContinueSend() {
      return continueSend;
   }

   /**
    * @param continueSend 是否重启续传，默认不续传
    */
   public void setContinueSend(boolean continueSend) {
      this.continueSend = continueSend;
   }

}
