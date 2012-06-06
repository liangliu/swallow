/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.dianping.swallow.common.codec;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

import java.io.ByteArrayOutputStream;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.caucho.hessian.io.Hessian2Output;
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
public class HessianEncoder extends OneToOneEncoder {

   private SerializerFactory factory = new SerializerFactory();;

   public HessianEncoder() {
      super();
   }

   @Override
   protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg instanceof Message) {// 对Message进行编码
         ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
         Hessian2Output h2o = new Hessian2Output(bos);
         h2o.setSerializerFactory(factory);
         h2o.writeObject(msg);
         h2o.flush();
         byte[] content = bos.toByteArray();
         return wrappedBuffer(content);
      }
      return msg;
   }
}
