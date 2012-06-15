package com.dianping.swallow.common.example.message2;

import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message2.SwallowMessage;

public class JsonToBean {

   public static void main(String[] args) throws Exception {

      //自定义bean
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();

      String json = "{\"generatedTime\":null,\"messageId\":null,\"properties\":{\"adasd\":\"dasd\"},\"retryCount\":0,\"version\":null,\"sha1\":null,\"content\":\"{\\\"a\\\":1,\\\"b\\\":\\\"b\\\"}\"}";
      SwallowMessage msg = jsonBinder.fromJson(json, SwallowMessage.class);
      System.out.println(msg);
      System.out.println(msg.deserializeAsBean(DemoBean.class));
   }

}
