package com.dianping.swallow.consumer.netty;



import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class MessageDecoder extends FrameDecoder {
	 
	@Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 4) {
            return null;//(1)
        }
        int dataLength = buffer.getInt(buffer.readerIndex());
        if (buffer.readableBytes() < dataLength + 4) {
            return null;//(2)
        }
        
        
 
        buffer.skipBytes(4);//(3)
        byte[] decoded = new byte[dataLength];
        buffer.readBytes(decoded);
        String msg = new String(decoded);//(4)
        return msg;
    }

}
