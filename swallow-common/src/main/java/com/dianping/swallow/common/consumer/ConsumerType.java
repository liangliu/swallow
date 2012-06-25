package com.dianping.swallow.common.consumer;

/**
 * consumerClient的类型，包括：1.SwallowC发送消息后接收ACK前更新MaxMessageId，2.SwallowC发送消息后，接收ACK后更新MaxMessageId
 * @author zhang.yu
 *
 */
public enum ConsumerType {

	UPDATE_BEFORE_ACK,
	UPDATE_AFTER_ACK
	
}
