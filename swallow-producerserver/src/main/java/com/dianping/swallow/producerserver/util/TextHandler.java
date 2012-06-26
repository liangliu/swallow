package com.dianping.swallow.producerserver.util;

import java.net.SocketAddress;
import java.util.Date;

import org.codehaus.jackson.map.ObjectMapper;

import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.producerserver.impl.TextObject;

public class TextHandler {
   //解析Text，成功返回Packet，失败返回null
   //可解析的格式为 send:topic=XX;content=XX;
   //TODO 用json
   @SuppressWarnings("null")
   public static PktTextMessage changeTextToPacket(SocketAddress addr, String jsonStr) {

      TextObject textObject = null;
      ObjectMapper mapper = new ObjectMapper();
      try {
         mapper.readValue(jsonStr, TextObject.class);
      } catch (Exception e) {
         return null;
      }

      SwallowMessage swallowMessage = new SwallowMessage();
      swallowMessage.setContent(textObject.getContent());
      swallowMessage.setGeneratedTime(new Date());
      swallowMessage.setSha1(SHAGenerater.generateSHA(swallowMessage.getContent()));

      PktTextMessage pkt = null;
      pkt = new PktTextMessage(textObject.getTopic(), swallowMessage, textObject.isACK());

      return pkt;
   }
}
