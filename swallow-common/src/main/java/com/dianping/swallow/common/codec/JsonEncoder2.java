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

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message2.SwallowMessage;

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
public class JsonEncoder2 extends OneToOneEncoder {

   public JsonEncoder2() {
      super();
   }

   @Override
   protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg instanceof SwallowMessage) {// 对Message进行编码
         JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
         String json = jsonBinder.toJson(msg);
         byte[] jsonBytes = json.getBytes(Charset.forName("UTF-8"));
         ChannelBuffer channelBuffer = ChannelBuffers.buffer(jsonBytes.length);
         channelBuffer.writeBytes(jsonBytes);
         return channelBuffer;
      }
      return msg;
   }
}
