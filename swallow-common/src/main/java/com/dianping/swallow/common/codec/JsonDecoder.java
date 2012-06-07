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
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import com.dianping.swallow.common.message.BeanMessage;
import com.dianping.swallow.common.message.BytesMessage;
import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.TextMessage;

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

   @SuppressWarnings("rawtypes")
   @Override
   protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (!(msg instanceof ChannelBuffer)) {
         return msg;
      }

      ChannelBuffer buf = (ChannelBuffer) msg;
      int contentTypeOrdinal = buf.readByte();
      String json = buf.toString(Charset.forName("UTF-8"));
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      Message message;
      if (contentTypeOrdinal == Message.ContentType.BeanMessage.ordinal()) {
         message = jsonBinder.fromJson(json, BeanMessage.class);
      } else if (contentTypeOrdinal == Message.ContentType.BytesMessage.ordinal()) {
         message = jsonBinder.fromJson(json, BytesMessage.class);
      } else if (contentTypeOrdinal == Message.ContentType.TextMessage.ordinal()) {
         message = jsonBinder.fromJson(json, TextMessage.class);
      } else {
         throw new IllegalArgumentException(
               "the first byte ahead of the json string must be a valid Message.ContentType enum type's ordinal.");
      }
      return message;
   }

}
