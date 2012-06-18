package com.dianping.swallow.common.codec;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message.SwallowMessage;

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
public class JsonDecoder extends OneToOneDecoder {

   public JsonDecoder() {
      super();
   }

   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (!(msg instanceof ChannelBuffer)) {
         return msg;
      }

      ChannelBuffer buf = (ChannelBuffer) msg;
      String json = buf.toString(Charset.forName("UTF-8"));
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      SwallowMessage message = jsonBinder.fromJson(json, SwallowMessage.class);
      return message;
   }

}
