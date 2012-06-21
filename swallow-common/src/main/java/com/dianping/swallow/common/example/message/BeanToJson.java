package com.dianping.swallow.common.example.message;

import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message.SwallowMessage;

public class BeanToJson {

   public static void main(String[] args) throws Exception {
      //自定义bean
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");

      SwallowMessage message = new SwallowMessage();
      message.setContent(demoBean);
      message.getProperties().setProperty("property-key", "property-value");

      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      String json = jsonBinder.toJson(message);
      System.out.println(json);

   }

}
