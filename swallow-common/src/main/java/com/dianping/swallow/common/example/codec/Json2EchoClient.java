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

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;

import com.dianping.swallow.common.codec.JsonDecoder2;

/**
 * 连接到服务器之后，会收到服务器发送的经过ProtobufVarint32LengthFieldPrepender和JsonEncoder编码后的消息 ，
 * 收到消息后ProtobufVarint32FrameDecoder和JsonDecoder解码，使用然后打印消息.
 */
public class Json2EchoClient {

   public static void main(String[] args) throws Exception {

      // Parse options.
      final String host = "localhost";
      final int port = 8080;

      // Configure the client.
      ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

      // Set up the pipeline factory.
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline p = Channels.pipeline();
            p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            p.addLast("jsonDecoder", new JsonDecoder2());
            p.addLast("handler", new EchoClientHandler());
            return p;
         }
      });

      // Start the connection attempt.
      ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

      // Wait until the connection is closed or the connection attempt fails.
      future.getChannel().getCloseFuture().awaitUninterruptibly();

      // Shut down thread pools to exit.
      bootstrap.releaseExternalResources();
   }
}
