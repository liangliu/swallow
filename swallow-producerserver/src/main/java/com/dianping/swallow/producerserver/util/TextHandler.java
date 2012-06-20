package com.dianping.swallow.producerserver.util;

import java.net.SocketAddress;
import java.util.Date;

import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktObjectMessage;
import com.dianping.swallow.common.packet.PktTextMessage;
import com.dianping.swallow.common.util.Destination;

public class TextHandler {
	//解析Text，成功返回Packet，失败返回null
	//可解析的格式为 send:topic=XX;content=XX;
	public static PktTextMessage changeTextToPacket(SocketAddress addr, String str){
		PktTextMessage pkt = null;
		
		//topic、content、isACK将组成pkt
		String	topic	= "";
		String	content	= "";
		boolean	isACK	= false;
		
		//将"send:XXXX"提取出来
		String[] strArray = str.trim().split(":");
		//如果String不以send开头或send：后无内容，则该Text报文无效
		if(!strArray[0].trim().toLowerCase().equals("send") || strArray.length < 2)	return null;
		
		//将提取出来的内容按照";"分段，解析出topic、content、isACK
		strArray = strArray[1].split(";");
		String field;
		for(String tmp : strArray){
			field = tmp.trim().toLowerCase();
			//对filed进行判断
			if(field.startsWith("topic=")){
				topic = field.substring("topic=".length()).trim();
			}else if(field.startsWith("content=")){
				content = field.substring("content=".length()).trim();
			}else if(field.startsWith("ack=")){
				//如果指定了ack位并将其设置为true，则发送ack，否则默认不发送ack
				if(field.substring("ack=".length()).trim().toLowerCase().equals("true")) isACK = true;
			}
			//出现不能识别的字段，Text报文无效
			else return null;
		}
		//topic || content为空，Text报文无效
		if(topic.equals("") || content.equals(""))	return null;
		
		SwallowMessage swallowMessage = new SwallowMessage();
		swallowMessage.setContent(content);
		swallowMessage.setGeneratedTime(new Date());
		swallowMessage.setSha1(SHAGenerater.generateSHA(content));
		
		PktObjectMessage objMsg = new PktObjectMessage(Destination.topic(topic), swallowMessage);
		
		pkt = new PktTextMessage(objMsg, isACK);
		
		return pkt;
	}
}
