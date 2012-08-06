package com.dianping.swallow.producer.impl.internal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
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
 * <tr>
 * <td>punishTimeout</td>
 * <td>=</td>
 * <td>500</td>
 * </tr>
 * </table>
 * 
 * @author tong.song
 */
public final class SwallowPigeonConfiguration {

   private static final Logger LOGGER                 = LoggerFactory.getLogger(SwallowPigeonConfiguration.class);

   public static final String  DEFAULT_SERVICE_NAME   = "http://service.dianping.com/swallowService/producerService_1.0.0"; //默认远程服务名称
   public static final String  DEFAULT_SERIALIZE      = "hessian";                                                         //默认序列化方式
   public static final int     DEFAULT_TIMEOUT        = 5000;                                                              //默认远程调用延时
   public static final boolean DEFAULT_IS_USE_LION    = true;                                                              //默认是否使用Lion以配置Swallow server地址
   public static final String  DEFAULT_HOSTS          = "127.0.0.1:4000";                                                  //默认Swallow server地址字符串
   public static final String  DEFAULT_WEIGHTS        = "1";                                                               //默认Swallow server权重
   public static final int     DEFAULT_PUNISH_TIMEOUT = 500;                                                               //默认失败重试延时基数

   private String              serviceName            = DEFAULT_SERVICE_NAME;                                              //远程调用服务名称，需与Swallow server端配置的服务名称完全一致
   private String              serialize              = DEFAULT_SERIALIZE;                                                 //序列化方式，共有四种：hessian,java,protobuf以及thrift
   private int                 timeout                = DEFAULT_TIMEOUT;                                                   //远程调用延时
   private boolean             useLion                = DEFAULT_IS_USE_LION;                                               //是否使用Lion以配置Swallow server地址，非开发环境只能使用Lion
   private String              hosts                  = DEFAULT_HOSTS;                                                     //Swallow server地址字符串，useLion为真时此项失效
   private String              weights                = DEFAULT_WEIGHTS;                                                   //Swallow server权重，范围从0-10，useLion为真时此项失效
   private int                 punishTimeout          = DEFAULT_PUNISH_TIMEOUT;                                            //失败重试延时基数

   public SwallowPigeonConfiguration() {
      //默认配置
   }

   @Override
   public String toString() {
      return "serviceName=" + serviceName + "; serialize=" + serialize + "; timeout=" + timeout + "; useLion="
            + useLion + (!useLion ? "; hosts=" + hosts + "; weights=" + weights : "") + "; punishTimeout="
            + punishTimeout;
   }

   private String getConfigInfo() {
      return "serviceName=" + serviceName + "; serialize=" + serialize + "; timeout=" + timeout + "; useLion="
            + useLion + (!useLion ? "; hosts=" + hosts + "; weights=" + weights : "") + "; punishTimeout="
            + punishTimeout;
   }

   @SuppressWarnings("rawtypes")
   public SwallowPigeonConfiguration(String configFile) {
      Properties props = new Properties();
      Class clazz = this.getClass();
      InputStream in = null;
      in = SwallowPigeonConfiguration.class.getClassLoader().getResourceAsStream(configFile);
      if (in == null) {
         LOGGER.warn("No ProducerFactory config file, use default values: [" + getConfigInfo() + "]");
         return;
      }
      try {
         props.load(in);
      } catch (IOException e) {
         LOGGER.error("Load property file failed, use default values: [" + getConfigInfo() + "]", e);
      } finally {
         if (in != null) {
            try {
               in.close();
            } catch (IOException e) {
               LOGGER.error("[Close inputstream failed.]", e);
            }
         }
      }
      for (String key : props.stringPropertyNames()) {
         Field field = null;
         try {
            field = clazz.getDeclaredField(key.trim());
         } catch (Exception e) {
            LOGGER.warn("[Unknow property found in " + configFile + ": " + key + ".]", e);
            continue;
         }
         field.setAccessible(true);

         if (field.getType().equals(Integer.TYPE)) {
            try {
               field.set(this, Integer.parseInt(props.getProperty(key).trim()));
            } catch (Exception e) {
               LOGGER.warn("[Can not parse property " + key + ".]", e);
               continue;
            }
         } else if (field.getType().equals(Boolean.TYPE)) {
            try {
               String str = props.getProperty(key).trim();
               field.set(this, ("false".equals(str) ? false : ("true".equals(str) ? true : DEFAULT_IS_USE_LION)));
            } catch (Exception e) {
               LOGGER.warn("[Can not parse property " + key + ".]", e);
               continue;
            }
         } else if (field.getType().equals(String.class)) {
            try {
               field.set(this, props.getProperty(key).trim());
            } catch (Exception e) {
               LOGGER.warn("[Can not parse property " + key + ".]", e);
               continue;
            }
         }
      }

      checkHostsAndWeights();
      checkSerialize();
      checkTimeout();
      checkUseLion();
      checkPunishTimeout();
      LOGGER.info("ProducerFactory configuration: [" + getConfigInfo() + "]");
   }

   /**
    * 检查序列化方式
    */
   private void checkSerialize() {
      if (!"hessian".equals(serialize) && !"java".equals(serialize) && !"protobuf".equals(serialize)
            && !"thrift".equals(serialize)) {
         LOGGER.warn("[Unrecognized serialize, use default value: " + DEFAULT_SERIALIZE + ".]");
         serialize = DEFAULT_SERIALIZE;
      }
   }

   /**
    * 检查hosts和weights，host个数与weight需一一对应，示例：hosts=
    * "127.0.0.1:4000,192.168.6.123:8000"; weights="1,2"
    */
   private void checkHostsAndWeights() {
      String[] hostSet = getHosts().trim().split(",");
      String[] weightSet = getWeights().trim().split(",");
      StringBuilder realHosts = new StringBuilder();
      StringBuilder realWeights = new StringBuilder();
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
            LOGGER.warn("[Unrecognized host address: " + host + ", or weight: " + weight + ", ignored it.]");
            continue;
         }
         realHosts.append(host + ",");
         realWeights.append(weight + ",");
      }
      if (realHosts.length() == 0) {
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
   private void checkTimeout() {
      if (timeout <= 0) {
         timeout = DEFAULT_TIMEOUT;
         LOGGER.warn("Timeout should be more than 0, use default value.");
      }
   }

   /**
    * 检查是否使用Lion，如果非开发环境，强制useLion=true
    */
   private void checkUseLion() {
      String env = EnvZooKeeperConfig.getEnv();
      if (!"dev".equals(env)) {
         LOGGER.warn("[Not dev, set useLion=" + DEFAULT_IS_USE_LION + ".]");
         useLion = DEFAULT_IS_USE_LION;
      }
   }

   private void checkPunishTimeout() {
      if (punishTimeout <= 0) {
         punishTimeout = DEFAULT_PUNISH_TIMEOUT;
         LOGGER.warn("PunishTimeout should be more than 0, use default value.");
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

   public int getPunishTimeout() {
      return punishTimeout;
   }

   public void setPunishTimeout(int punishTimeout) {
      this.punishTimeout = punishTimeout;
      checkPunishTimeout();
   }
}
