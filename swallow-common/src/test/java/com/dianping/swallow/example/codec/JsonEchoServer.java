package com.dianping.swallow.example.codec;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import com.dianping.swallow.common.internal.codec.JsonEncoder;
import com.dianping.swallow.common.internal.message.SwallowMessage;

/**
 * 收到客户端的连接后，发送经过ProtobufVarint32LengthFieldPrepender和JsonEncoder编码后的消息。
 */
public class JsonEchoServer {

   public static void main(String[] args) throws Exception {
      // Configure the server.
      ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

      // Set up the pipeline factory.
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline p = Channels.pipeline();
            p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            p.addLast("jsonEncoder", new JsonEncoder(SwallowMessage.class));
            p.addLast("handler", new EchoServerHandler());
            return p;
         }
      });

      // Bind and start to accept incoming connections.
      bootstrap.bind(new InetSocketAddress(8080));
   }
}
