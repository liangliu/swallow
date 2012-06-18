package com.dianping.swallow.common.codec;

import java.io.ByteArrayInputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.SerializerFactory;
import com.dianping.swallow.common.message.Message;

/**
 * 用法:
 * 
 * <pre>
 * ChannelPipeline p = pipeline();
 * p.addLast(&quot;frameDecoder&quot;, new ProtobufVarint32FrameDecoder());
 * p.addLast(&quot;hessianDecoder&quot;, new HessianDecoder());
 * 
 * p.addLast(&quot;frameEncoder&quot;, new ProtobufVarint32LengthFieldPrepender());
 * p.addLast(&quot;hessianEncoder&quot;, new HessianEncoder());
 * 
 * p.addLast(&quot;handler&quot;, new XXClientHandler());
 * </pre>
 */
public class HessianDecoder extends OneToOneDecoder {

   private SerializerFactory factory = new SerializerFactory(); ;

   public HessianDecoder() {
      super();
   }

   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (!(msg instanceof ChannelBuffer)) {
         return msg;
      }

      ChannelBuffer buf = (ChannelBuffer) msg;
      if (buf.hasArray()) {
         ByteArrayInputStream bis = new ByteArrayInputStream(buf.array(), buf.arrayOffset() + buf.readerIndex(),
               buf.readableBytes());
         Hessian2Input in = new Hessian2Input(bis);
         in.setSerializerFactory(factory);
         return in.readObject(Message.class);
      } else {
         Hessian2Input in = new Hessian2Input(new ChannelBufferInputStream((ChannelBuffer) msg));
         in.setSerializerFactory(factory);
         return in.readObject(Message.class);
      }
   }

}
