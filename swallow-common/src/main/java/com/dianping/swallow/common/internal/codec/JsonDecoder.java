package com.dianping.swallow.common.internal.codec;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;


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
public class JsonDecoder extends OneToOneDecoder {

   private Class clazz;

   public JsonDecoder(Class clazz) {
      super();
      this.clazz = clazz;
   }

   @SuppressWarnings("unchecked")
   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (!(msg instanceof ChannelBuffer)) {
         return msg;
      }

      ChannelBuffer buf = (ChannelBuffer) msg;
      String json = buf.toString(Charset.forName("UTF-8"));
      JsonBinder jsonBinder = JsonBinder.getNonEmptyBinder();
      return jsonBinder.fromJson(json, clazz);
   }

}
