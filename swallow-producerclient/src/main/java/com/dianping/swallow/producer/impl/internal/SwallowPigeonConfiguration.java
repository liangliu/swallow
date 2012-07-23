package com.dianping.swallow.producer.impl.internal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.lion.EnvZooKeeperConfig;

/**
 * ProducerFactory配置，默认构造函数生成的配置为：<br />
 * <br />
 * <table>
 * <tr>
 * <td>serviceName</td>
 * <td>=</td>
 * <td>"http://service.dianping.com/swallowService/producerService_1.0.0"</td>
 * </tr>
 * <tr>
 * <td>serialize</td>
 * <td>=</td>
 * <td>"hessian"</td>
 * </tr>
 * <tr>
 * <td>timeout</td>
 * <td>=</td>
 * <td>5000</td>
 * </tr>
 * <tr>
 * <td>useLion</td>
 * <td>=</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td>hosts</td>
 * <td>=</td>
 * <td>"127.0.0.1:4000"</td>
 * </tr>
 * <tr>
 * <td>weights</td>
 * <td>=</td>
 * <td>"1"</td>
 * </tr>
 * </table>
 * 
 * @author tong.song
 */
public class SwallowPigeonConfiguration {

   private static final Logger logger               = LoggerFactory.getLogger(SwallowPigeonConfiguration.class);

   public static final String  DEFAULT_SERVICE_NAME = "http://service.dianping.com/swallowService/producerService_1.0.0"; //默认远程服务名称
   public static final String  DEFAULT_SERIALIZE    = "hessian";                                                         //默认序列化方式
   public static final int     DEFAULT_TIMEOUT      = 5000;                                                              //默认远程调用延时
   public static final boolean DEFAULT_IS_USE_LION  = true;                                                              //默认是否使用Lion以配置Swallow server地址
   public static final String  DEFAULT_HOSTS        = "127.0.0.1:4000";                                                  //默认Swallow server地址字符串
   public static final String  DEFAULT_WEIGHTS      = "1";                                                               //默认Swallow server权重

   private String              serviceName          = DEFAULT_SERVICE_NAME;                                              //远程调用服务名称，需与Swallow server端配置的服务名称完全一致
   private String              serialize            = DEFAULT_SERIALIZE;                                                 //序列化方式，共有四种：hessian,java,protobuf以及thrift
   private int                 timeout              = DEFAULT_TIMEOUT;                                                   //远程调用延时
   private boolean             useLion              = DEFAULT_IS_USE_LION;                                               //是否使用Lion以配置Swallow server地址，非开发环境只能使用Lion
   protected String            hosts                = DEFAULT_HOSTS;                                                     //Swallow server地址字符串，useLion为真时此项失效
   protected String            weights              = DEFAULT_WEIGHTS;                                                   //Swallow server权重，范围从0-10，useLion为真时此项失效

   public SwallowPigeonConfiguration() {
      //默认配置
   }

   @Override
   public String toString() {
      return "serviceName=" + serviceName + "; serialize=" + serialize + "; timeout=" + timeout + "; useLion="
            + useLion + (useLion == false ? "; hosts=" + hosts + "; weights=" + weights : "");
   }

   @SuppressWarnings("rawtypes")
   public SwallowPigeonConfiguration(String configFile) {
      Properties props = new Properties();
      Class clazz = this.getClass();
      InputStream in = null;
      in = SwallowPigeonConfiguration.class.getClassLoader().getResourceAsStream(configFile);
      if (in == null) {
         logger.warn("No ProducerFactory config file, use default values: [" + this.toString() + "]");
         return;
      }
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
      System.out.println(props.toString());
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
               String str = props.getProperty(key).trim();
               field.set(this, ("false".equals(str) ? false : ("true".equals(str) ? true : DEFAULT_IS_USE_LION)));
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

      checkHostsAndWeights();
      checkSerialize();
      checkTimeout();
      checkUseLion();
      logger.info("ProducerFactory configuration: [" + this.toString() + "]");
   }

   /**
    * 检查序列化方式
    */
   protected void checkSerialize() {
      if (!"hessian".equals(serialize) && !"java".equals(serialize) && !"protobuf".equals(serialize)
            && !"thrift".equals(serialize)) {
         logger.warn("[Unrecognized serialize, use default value: " + DEFAULT_SERIALIZE + ".]");
         serialize = DEFAULT_SERIALIZE;
      }
   }

   /**
    * 检查hosts和weights，host个数与weight需一一对应，示例：hosts=
    * "127.0.0.1:4000,192.168.6.123:8000"; weights="1,2"
    */
   protected void checkHostsAndWeights() {
      String[] hostSet = getHosts().trim().split(",");
      String[] weightSet = getWeights().trim().split(",");
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
         this.hosts = DEFAULT_HOSTS;
         this.weights = DEFAULT_WEIGHTS;
      } else {
         this.hosts = realHosts.substring(0, realHosts.length() - 1);
         this.weights = realWeights.substring(0, realWeights.length() - 1);
      }
   }

   /**
    * 检查Timeout是否合法，Timeout>0
    */
   protected void checkTimeout() {
      if (timeout <= 0)
         timeout = DEFAULT_TIMEOUT;
   }

   /**
    * 检查是否使用Lion，如果非开发环境，强制useLion=true
    */
   protected void checkUseLion() {
      String env = EnvZooKeeperConfig.getEnv();
      if (!"dev".equals(env)) {
         logger.warn("[Not dev, set useLion=" + DEFAULT_IS_USE_LION + ".]");
         useLion = DEFAULT_IS_USE_LION;
      }
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
      checkSerialize();
   }

   public int getTimeout() {
      return timeout;
   }

   public void setTimeout(int timeout) {
      this.timeout = timeout;
      checkTimeout();
   }

   public boolean isUseLion() {
      return useLion;
   }

   public void setUseLion(boolean useLion) {
      this.useLion = useLion;
      checkUseLion();
   }

   public String getHosts() {
      return hosts;
   }

   public String getWeights() {
      return weights;
   }

   public void setHostsAndWeights(String hosts, String weights) {
      this.hosts = hosts;
      this.weights = weights;
      checkHostsAndWeights();
   }
}
