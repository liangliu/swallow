package com.dianping.swallow.common.internal.codec;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Assert;
import org.junit.Test;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.SerializerFactory;
import com.dianping.swallow.common.internal.codec.HessianEncoder;
import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.message.Message;

public class HessianEncoderTest {

   private SerializerFactory factory = new SerializerFactory();

   @Test
   public void testEncode1() throws Exception {
      //构造序列化后的hessian字节码
      SwallowMessage msg = new SwallowMessage();
      msg.setGeneratedTime(new Date());
      msg.setMessageId(123L);
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      msg.setContent("content");
      HessianEncoder hessianEncoder = new HessianEncoder();
      ChannelBuffer channelBuffer = (ChannelBuffer)hessianEncoder.encode(null, null, msg);
      //解码
      InputStream is = new ByteArrayInputStream(channelBuffer.toByteBuffer().array());
      Hessian2Input h2i = new Hessian2Input(is);
      h2i.setSerializerFactory(factory);
      Message actualMsg = (Message) h2i.readObject();
      h2i.close();
      //assert
      Assert.assertEquals(msg, actualMsg);
   }
   
   @Test
   public void testEncode2() throws Exception {
      Object o = new Object();
      HessianEncoder hessianEncoder = new HessianEncoder();
      Assert.assertEquals(o,hessianEncoder.encode(null, null, o));
   }

}
