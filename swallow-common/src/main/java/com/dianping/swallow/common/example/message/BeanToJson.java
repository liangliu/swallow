package com.dianping.swallow.common.example.message;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.dianping.swallow.common.message.BeanMessage;
import com.dianping.swallow.common.message.JsonBinder;

public class BeanToJson {

   public static void main(String[] args) throws Exception {
      //自定义bean
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");

      BeanMessage message = new BeanMessage();
      message.writeBeanAsJsonString(demoBean);
      Properties properties = new Properties();
      properties.setProperty("adasd", "dasd");
      message.setProperties(properties);

      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      String json = jsonBinder.toJson(message);
      System.out.println(json);
      //Map
      Map<String, String> map = new HashMap<String, String>();
      map.put("key", "value");
      System.out.println(jsonBinder.toJson(map));

   }

}
