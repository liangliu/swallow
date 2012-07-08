package com.dianping.swallow.common.internal.codec;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Assert;
import org.junit.Test;

import com.dianping.swallow.common.internal.codec.JsonBinder;
import com.dianping.swallow.common.internal.codec.JsonEncoder;
import com.dianping.swallow.common.internal.message.SwallowMessage;

public class JsonEncoderTest {

   @Test
   public void testEncode1() throws Exception {
      //构造序列化后的json
      SwallowMessage msg = new SwallowMessage();
      msg.setGeneratedTime(new Date());
      msg.setMessageId(123L);
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      msg.setContent("content");
      JsonEncoder jsonEncoder = new JsonEncoder(SwallowMessage.class);
      ChannelBuffer channelBuffer = (ChannelBuffer) jsonEncoder.encode(null, null, msg);
      //解码
      JsonBinder jsonBinder = JsonBinder.buildBinder();
      SwallowMessage actualMsg = jsonBinder.fromJson(channelBuffer.toString(Charset.forName("UTF-8")),
            SwallowMessage.class);

      //assert
      Assert.assertEquals(msg, actualMsg);
   }

   @Test
   public void testEncode2() throws Exception {
      Object o = new Object();
      JsonEncoder jsonEncoder = new JsonEncoder(SwallowMessage.class);
      Assert.assertEquals(o, jsonEncoder.encode(null, null, o));
   }

}
