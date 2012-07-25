package com.dianping.swallow.producer;

import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;

/**
 * 不同工作模式Producer的处理方法接口
 * 
 * @author tong.song
 */
public interface ProducerHandler {
   /**
    * 根据Producer工作模式的不同，以不同的方式发送打包好的消息至Swallow服务器
    * 
    * @param pkt 打包好的消息
    * @return 根据Producer工作模式不同，返回值也不同
    * @throws SendFailedException 发送失败抛出此异常
    */
   Packet doSendMsg(Packet pkt) throws SendFailedException;
}
