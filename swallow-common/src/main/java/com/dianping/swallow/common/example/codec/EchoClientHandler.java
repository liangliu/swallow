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
package com.dianping.swallow.common.example.codec;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class EchoClientHandler extends SimpleChannelUpstreamHandler {
   private final ChannelBuffer helloMessage;

   {
      helloMessage = ChannelBuffers.buffer(1);
      for (int i = 0; i < helloMessage.capacity(); i++) {
         helloMessage.writeByte((byte) i);
      }
   }

   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      // 连接之后，主动发送数据给服务器
      e.getChannel().write(helloMessage);
   }

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // 收到服务器发过来的消息，打印出来
      System.out.println(e.getMessage());
   }

}
