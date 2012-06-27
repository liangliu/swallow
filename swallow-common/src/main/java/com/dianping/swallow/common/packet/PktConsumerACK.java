package com.dianping.swallow.common.packet;

/**
 * 
 * @author zhang.yu
 *
 */
public final class PktConsumerACK extends Packet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2019071544984869600L;
	
	private Long messageId;

	public Long getMessageId() {
		return messageId;
	}

	public void setMessageId(Long messageId) {
		this.messageId = messageId;
	}

	public PktConsumerACK(Long messageId) {
		this.setPacketType(PacketType.CONSUMER_ACK);
		this.messageId = messageId;
	}

}
