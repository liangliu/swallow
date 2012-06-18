package com.dianping.swallow.producerserver.util;

import java.net.SocketAddress;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.common.util.Destination;

public class TextHandler {
	//解析Text，成功返回Packet，失败返回null
	public static Packet changeTextToPacket(SocketAddress addr, String str){
		PktStringMessage pkt = null;
		String destName = null;
		String content = null;
		str = str.trim();
		String[] strArray = str.split(":");
		if(!strArray[0].trim().toLowerCase().equals("send"))	return null;
		
		strArray = strArray[1].split(";");
		String subStr;
		for(String tmp : strArray){
			subStr = tmp.trim().toLowerCase();
			if(subStr.startsWith("topic=")){
				destName = tmp.substring("topic=".length());
			}else if(subStr.startsWith("content=")){
				content = tmp.substring("content=".length());
			}else return null;
		}
		
		pkt = new PktStringMessage(Destination.topic(destName), content);
		return pkt;
	}
}
