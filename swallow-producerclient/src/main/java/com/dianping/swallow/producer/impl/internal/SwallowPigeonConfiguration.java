package com.dianping.swallow.producer.impl.internal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.producer.exceptions.SendFailedException;

public final class SwallowPigeonConfiguration {

   private static final Logger logger               = LoggerFactory.getLogger(SwallowPigeonConfiguration.class);

   public static final String  DEFAULT_SERVICE_NAME = "remoteService";
   public static final String  DEFAULT_SERIALIZE    = "hessian";
   public static final int     DEFAULT_TIMEOUT      = 5000;
   public static final boolean DEFAULT_IS_USE_LION  = true;
   public static final String  DEFAULT_HOSTS        = "127.0.0.1:4000";
   public static final String  DEFAULT_WEIGHTS      = "1";

   private String              serviceName          = DEFAULT_SERVICE_NAME;
   private String              serialize            = DEFAULT_SERIALIZE;
   private int                 timeout              = DEFAULT_TIMEOUT;
   private boolean             useLion              = DEFAULT_IS_USE_LION;
   private String              hosts                = DEFAULT_HOSTS;
   private String              weights              = DEFAULT_WEIGHTS;

   public SwallowPigeonConfiguration() {
      //默认配置
   }

   @SuppressWarnings("rawtypes")
   public SwallowPigeonConfiguration(String configFile) {
      Properties props = new Properties();
      Class clazz = this.getClass();
      InputStream in = null;
      in = SwallowPigeonConfiguration.class.getClassLoader().getResourceAsStream(configFile);
      if (in == null)
         return;
      try {
         props.load(in);
      } catch (IOException e) {
         logger.error("[Load property file failed.]", e);
         e.printStackTrace();
      } finally {
         if (in != null) {
            try {
               in.close();
            } catch (IOException e) {
               logger.error("[Close inputstream failed.]", e);
            }
         }
      }

      for (String key : props.stringPropertyNames()) {
         Field field = null;
         try {
            field = clazz.getDeclaredField(key.trim());
         } catch (Exception e) {
            logger.warn("[Unknow property found in " + configFile + ": " + key + ".]", e);
            continue;
         }
         field.setAccessible(true);

         if (field.getType().equals(Integer.TYPE)) {
            try {
               field.set(this, Integer.parseInt(props.getProperty(key).trim()));
            } catch (Exception e) {
               logger.warn("[Can not parse property " + key + ".]", e);
               continue;
            }
         } else if (field.getType().equals(Boolean.TYPE)) {
            try {
               field.set(this, Boolean.parseBoolean(props.getProperty(key).trim()));
            } catch (Exception e) {
               logger.warn("[Can not parse property " + key + ".]", e);
               continue;
            }
         } else if (field.getType().equals(String.class)) {
            try {
               field.set(this, props.getProperty(key).trim());
            } catch (Exception e) {
               logger.warn("[Can not parse property " + key + ".]", e);
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
      checkParams();
   }

   private void checkParams() {
      //检查序列化方式
      if (!"hessian".equals(serialize) && !"java".equals(serialize) && !"protobuf".equals(serialize)
            && !"thrift".equals(serialize)) {
         logger.warn("[Unrecognized serialize, use default value: " + DEFAULT_SERIALIZE + ".]");
         serialize = DEFAULT_SERIALIZE;
      }

      //检查hosts和weights
      String[] hostSet = hosts.trim().split(",");
      String[] weightSet = weights.trim().split(",");
      String realHosts = "";
      String realWeights = "";
      String host;
      String weight;
      for (int idx = 0; idx < hostSet.length; idx++) {
         host = hostSet[idx];
         if (idx >= weightSet.length) {
            weight = "-1";
         } else {
            weight = weightSet[idx];
         }
         if (!host.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d{4,5}") || !weight.matches("[1-9]|10")) {
            logger.warn("[Unrecognized host address: " + host + ", or weight: " + weight + ", ignored it.]");
            continue;
         }
         realHosts += host + ",";
         realWeights += weight + ",";
      }
      if (realHosts == "") {
         hosts = DEFAULT_HOSTS;
         weights = DEFAULT_WEIGHTS;
      } else {
         hosts = realHosts.substring(0, realHosts.length() - 1);
         weights = realWeights.substring(0, realWeights.length() - 1);
      }

      //检查Timeout
      if (timeout <= 0)
         timeout = DEFAULT_TIMEOUT;
   }

   public String getServiceName() {
      return serviceName;
   }

   public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
   }

   public String getSerialize() {
      return serialize;
   }

   public void setSerialize(String serialize) {
      this.serialize = serialize;
      checkParams();
   }

   public int getTimeout() {
      return timeout;
   }

   public void setTimeout(int timeout) {
      this.timeout = timeout;
      checkParams();
   }

   public boolean isUseLion() {
      return useLion;
   }

   public void setUseLion(boolean useLion) {
      this.useLion = useLion;
   }

   public String getHosts() {
      return hosts;
   }

   public String getWeights() {
      return weights;
   }

   public void setHostsAndWeights(String hosts, String weights) throws SendFailedException {
      this.hosts = hosts;
      this.weights = weights;
      checkParams();
      throw new SendFailedException(hosts + " " + weights);
   }
}
