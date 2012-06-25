package com.dianping.swallow.common.packet;

import com.dianping.swallow.common.message.Destination;

public class PktObjectMessage extends Packet implements Message{
	private Object content;
	private Destination dest;
	
	public PktObjectMessage(Destination dest, Object content){
		super.setPacketType(PacketType.OBJECT_MSG);
		
		this.dest = dest;
		this.content = content;
	}
	
	@Override
	public Object getContent() {
		// TODO Auto-generated method stub
		return content;
	}

	@Override
	public Destination getDestination() {
		// TODO Auto-generated method stub
		return dest;
	}
}
