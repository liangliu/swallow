package com.dianping.swallow.common.codec;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.HashMap;

import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;

public class HessianDecoderTest {

   private SerializerFactory factory = new SerializerFactory();

   @Test
   public void testDecode() throws Exception {
      //构造序列化后的hessian字节码
      SwallowMessage msg = new SwallowMessage();
      msg.setGeneratedTime(new Date());
      msg.setMessageId(123L);
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("property-key", "property-value");
      msg.setContent("content");
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      Hessian2Output h2o = new Hessian2Output(bos);
      h2o.setSerializerFactory(factory);
      h2o.writeObject(msg);
      h2o.flush();
      h2o.close();
      byte[] content = bos.toByteArray();
      //使用HessianDecoder解码
      HessianDecoder hessianDecoder = new HessianDecoder();
      Message actualMsg = (Message) hessianDecoder.decode(null, null, ChannelBuffers.wrappedBuffer(content));
      //assert
      Assert.assertEquals(msg, actualMsg);
   }

   @Test
   public void testEncode2() throws Exception {
      Object o = new Object();
      HessianDecoder hessianDecoder = new HessianDecoder();
      Assert.assertEquals(o, hessianDecoder.decode(null, null, o));
   }

}
