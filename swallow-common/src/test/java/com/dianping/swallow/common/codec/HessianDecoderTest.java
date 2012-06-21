package com.dianping.swallow.common.codec;

import java.io.ByteArrayOutputStream;
import java.util.Date;

import org.jboss.netty.buffer.ChannelBuffer;
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
      msg.getProperties().put("key", "value");
      msg.setContent("content");
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      Hessian2Output h2o = new Hessian2Output(bos);
      h2o.setSerializerFactory(factory);
      h2o.writeObject(msg);
      h2o.flush();
      byte[] content = bos.toByteArray();
      ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(content);
      //使用HessianDecoder解码
      HessianDecoder hessianDecoder = new HessianDecoder();
      Message actualMsg = (Message) hessianDecoder.decode(null, null, channelBuffer);
      //assert
      Assert.assertEquals(msg, actualMsg);
      System.out.println(actualMsg);
   }

}
