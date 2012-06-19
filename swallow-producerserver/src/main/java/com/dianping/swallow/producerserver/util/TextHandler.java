package com.dianping.swallow.producerserver.util;

import java.net.SocketAddress;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.common.util.Destination;

public class TextHandler {
	//解析Text，成功返回Packet，失败返回null
	//可解析的格式为 send:topic=XX;content=XX;
	public static PktTextMessage changeTextToPacket(SocketAddress addr, String str){
		PktTextMessage pkt = null;
		String topic = "";
		String content = "";
		boolean isACK = false;
		str = str.trim();
		String[] strArray = str.split(":");
		if(!strArray[0].trim().toLowerCase().equals("send") || strArray.length < 2)	return null;
		
		strArray = strArray[1].split(";");
		String subStr;
		for(String tmp : strArray){
			subStr = tmp.trim().toLowerCase();
			if(subStr.startsWith("topic=")){
				topic = subStr.substring("topic=".length()).trim();
			}else if(subStr.startsWith("content=")){
				content = subStr.substring("content=".length()).trim();
			}else if(subStr.startsWith("ack=")){
				if(subStr.substring("ack=".length()).trim().toLowerCase().equals("true")) isACK = true;
			}
			else return null;
		}
		//topic || content为空，返回null
		if(topic.equals("") || content.equals(""))	return null;
		
		pkt = new PktTextMessage(Destination.topic(topic), content, isACK);
		return pkt;
	}
}
