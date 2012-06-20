package com.dianping.swallow.common.message;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jackson的简单封装.
 * 
 * @see <a
 *      href="http://code.google.com/p/springside/source/browse/springside3/trunk/modules/core/src/main/java/org/springside/modules/utils/mapping/JsonBinder.java?spec=svn1465&r=1465">SpringSide</a>
 */
public class JsonBinder {

   private static Logger logger = LoggerFactory.getLogger(JsonBinder.class);

   private ObjectMapper  mapper;

   public JsonBinder(Inclusion inclusion) {
      mapper = new ObjectMapper();
      //设置输出时包含属性的风格
      mapper.getSerializationConfig().withSerializationInclusion(inclusion);
      //设置输入时忽略在JSON字符串中存在但Java对象实际没有的属性
      mapper.getDeserializationConfig().without(
            org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
   }

   /**
    * 创建输出全部属性到Json字符串的Binder.
    */
   public static JsonBinder buildNormalBinder() {
      return new JsonBinder(Inclusion.ALWAYS);
   }

   /**
    * 创建只输出非空属性到Json字符串的Binder.
    */
   public static JsonBinder buildNonNullBinder() {
      return new JsonBinder(Inclusion.NON_NULL);
   }

   /**
    * 创建只输出初始值被改变的属性到Json字符串的Binder.
    */
   public static JsonBinder buildNonDefaultBinder() {
      return new JsonBinder(Inclusion.NON_DEFAULT);
   }

   /**
    * 如果JSON字符串为Null或"null"字符串,返回Null. 如果JSON字符串为"[]",返回空集合.
    * 如需读取集合如List/Map,且不是List<String>这种简单类型时使用如下语句: List<MyBean> beanList =
    * binder.getMapper().readValue(listString, new TypeReference<List<MyBean>>()
    * {});
    */
   public <T> T fromJson(String jsonString, Class<T> clazz) {
      if (jsonString == null || "".equals(jsonString.trim())) {
         return null;
      }

      try {
         return mapper.readValue(jsonString, clazz);
      } catch (IOException e) {
         logger.warn("parse json string error:" + jsonString, e);
         return null;
      }
   }

   /**
    * 如果对象为Null,返回"null". 如果集合为空集合,返回"[]".
    */
   public String toJson(Object object) {

      try {
         return mapper.writeValueAsString(object);
      } catch (IOException e) {
         logger.warn("write to json string error:" + object, e);
         return null;
      }
   }

   /**
    * 取出Mapper做进一步的设置或使用其他序列化API.
    */
   public ObjectMapper getMapper() {
      return mapper;
   }
}
