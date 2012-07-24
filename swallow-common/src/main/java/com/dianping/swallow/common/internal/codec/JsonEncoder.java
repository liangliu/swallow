package com.dianping.swallow.common.internal.codec;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;


/**
 * 用法:
 * 
 * <pre>
 * ChannelPipeline p = pipeline();
 * p.addLast(&quot;frameDecoder&quot;, new ProtobufVarint32FrameDecoder());
 * p.addLast(&quot;jsonDecoder&quot;, new JsonDecoder());
 * 
 * p.addLast(&quot;frameEncoder&quot;, new ProtobufVarint32LengthFieldPrepender());
 * p.addLast(&quot;jsonEncoder&quot;, new JsonEncoder());
 * 
 * p.addLast(&quot;handler&quot;, new XXClientHandler());
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class JsonEncoder extends OneToOneEncoder {
   private Class clazz;

   public JsonEncoder(Class clazz) {
      super();
      this.clazz = clazz;
   }

   @Override
   protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg.getClass() == clazz) {// 对Message进行编码
         JsonBinder jsonBinder = JsonBinder.getNonEmptyBinder();
         String json = jsonBinder.toJson(msg);
         byte[] jsonBytes = json.getBytes(Charset.forName("UTF-8"));
         ChannelBuffer channelBuffer = ChannelBuffers.buffer(jsonBytes.length);
         channelBuffer.writeBytes(jsonBytes);
         return channelBuffer;
      }
      return msg;
   }

}
