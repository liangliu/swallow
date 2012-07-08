package com.dianping.swallow.common.internal.consumer;

/**
 * 接收到ack后的处理类型，1.
 * 
 * @author zhang.yu
 */
public enum ACKHandlerType {

   SEND_MESSAGE,
   CLOSE_CHANNEL,
   NO_SEND

}
