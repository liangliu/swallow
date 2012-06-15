package com.dianping.swallow.common.codec;

import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.Properties;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.TextMessage;

@SuppressWarnings("rawtypes")
public class HessianDecoderTest {

   private SerializerFactory factory = new SerializerFactory();

   @Test
   public void testDecode() throws Exception {
      //构造序列化后的hessian字节码
      TextMessage msg = new TextMessage();
      msg.setGeneratedTime(new Date());
      msg.setMessageId(123L);
      Properties properties = new Properties();
      properties.put("key", "value");
      msg.setProperties(properties);
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
