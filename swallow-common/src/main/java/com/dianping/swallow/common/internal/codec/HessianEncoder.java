package com.dianping.swallow.common.internal.codec;


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
public class HessianEncoder {

//   private SerializerFactory factory = new SerializerFactory();
//
//   public HessianEncoder() {
//      super();
//   }
//
//   @Override
//   protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
//      if (msg instanceof Message) {// 对Message进行编码
//         ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
//         Hessian2Output h2o = new Hessian2Output(bos);
//         h2o.setSerializerFactory(factory);
//         h2o.writeObject(msg);
//         h2o.flush();
//         byte[] content = bos.toByteArray();
//         return ChannelBuffers.wrappedBuffer(content);
//      }
//      return msg;
//   }
}
