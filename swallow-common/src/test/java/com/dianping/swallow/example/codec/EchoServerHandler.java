package com.dianping.swallow.example.codec;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.dianping.swallow.example.message.DemoBean;
import com.dianping.swallow.common.message.SwallowMessage;

public class EchoServerHandler extends SimpleChannelUpstreamHandler {

   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      //服务器发送消息

      // 测试SwallowMessage
      DemoBean demoBean = new DemoBean();
      demoBean.setA(1);
      demoBean.setB("b");
      SwallowMessage m = new SwallowMessage();
      m.setContent(demoBean);

      e.getChannel().write(m);
   }

}
