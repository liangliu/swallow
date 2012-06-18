package com.dianping.swallow.common.example.message2;

import java.util.Properties;

import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message2.SwallowMessage;

public class BeanToJson {

   public static void main(String[] args) throws Exception {
      //自定义bean
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");

      SwallowMessage message = new SwallowMessage();
      message.setContentAsJsonString(demoBean);
      Properties properties = new Properties();
      properties.setProperty("property-key", "property-value");
      message.setProperties(properties);

      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      String json = jsonBinder.toJson(message);
      System.out.println(json);

   }

}
