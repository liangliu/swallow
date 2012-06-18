package com.dianping.swallow.producerserver.util;

import java.net.SocketAddress;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktStringMessage;
import com.dianping.swallow.common.util.Destination;

public class TextHandler {
	//解析Text，成功返回Packet，失败返回null
	//可解析的格式为 send:topic=XX;content=XX;
	public static Packet changeTextToPacket(SocketAddress addr, String str){
		PktStringMessage pkt = null;
		String topic = "";
		String content = "";
		str = str.trim();
		String[] strArray = str.split(":");
		if(!strArray[0].trim().toLowerCase().equals("send") || strArray.length < 2)	return null;
		
		strArray = strArray[1].split(";");
		String subStr;
		for(String tmp : strArray){
			subStr = tmp.trim().toLowerCase();
			if(subStr.startsWith("topic=")){
				topic = tmp.substring("topic=".length());
			}else if(subStr.startsWith("content=")){
				content = tmp.substring("content=".length());
			}else return null;
		}
		//topic || content为空，返回null
		if(topic.equals("") || content.equals(""))	return null;
		
		pkt = new PktStringMessage(Destination.topic(topic), content);
		return pkt;
	}
}
