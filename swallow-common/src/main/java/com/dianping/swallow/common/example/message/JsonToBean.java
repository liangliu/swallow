package com.dianping.swallow.common.example.message;

import java.util.HashMap;
import java.util.Map;

import com.dianping.swallow.common.message.BeanMessage;
import com.dianping.swallow.common.message.JsonBinder;

public class JsonToBean {

   @SuppressWarnings("unchecked")
   public static void main(String[] args) throws Exception {

      //自定义bean
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();

      String json = "{\"generatedTime\":null,\"messageId\":null,\"properties\":{\"adasd\":\"dasd\"},\"retryCount\":0,\"version\":null,\"contentType\":\"BeanMessage\",\"content\":\"{\\\"a\\\":1,\\\"b\\\":\\\"b\\\"}\"}";
      BeanMessage bm = jsonBinder.fromJson(json, BeanMessage.class);
      System.out.println(bm);
      System.out.println(bm.readBean(DemoBean.class));
      //Map
      json = "{\"key\":\"value\"}";
      Map<String, String> map = jsonBinder.fromJson(json, HashMap.class);
      System.out.println(map);
   }

}
