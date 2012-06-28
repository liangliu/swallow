package com.dianping.swallow.common.codec;

import java.util.HashMap;

import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.example.message.DemoBean;

public class JsonDecoderTest {

   @Test
   public void testDecode() throws Exception {
      //构造序列化后的json
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");
      SwallowMessage message = new SwallowMessage();
      message.setContent(demoBean);
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      message.setProperties(map);
      JsonBinder jsonBinder = JsonBinder.buildBinder();
      String json = jsonBinder.toJson(message);
      //使用HessianDecoder解码
      JsonDecoder jsonDecoder = new JsonDecoder(SwallowMessage.class);
      Message actualMessage = (Message) jsonDecoder.decode(null, null,
            ChannelBuffers.wrappedBuffer(json.getBytes("UTF-8")));
      //assert
      Assert.assertEquals(message, actualMessage);
   }

   @Test
   public void testEncode2() throws Exception {
      Object o = new Object();
      JsonDecoder jsonDecoder = new JsonDecoder(SwallowMessage.class);
      Assert.assertEquals(o, jsonDecoder.decode(null, null, o));
   }

}
