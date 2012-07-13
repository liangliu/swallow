package com.dianping.swallow.common.consumer;

/**
 * consumerClient的类型，包括3种类型：<br>
 * 1.AT_MOST：尽量保证消息最多消费一次，不出现重复消费（注意：只是尽量保证，而非绝对保证。）<br>
 * 2.AT_LEAST：尽量保证消息最少消费一次，不出现消息丢失的情况（注意：只是尽量保证，而非绝对保证。）<br>
 * 3.NON_DURABLE：临时的消费类型，从当前的消息开始消费，不会对消费状态进行持久化，Server重启后将重新开始。
 * 
 * @author zhang.yu
 */
public enum ConsumerType {
   //AT_MOST实现原理:SwallowC发送消息后，接收ACK前就更新MaxMessageId
   //AT_LEAST实现原理:SwallowC发送消息后，接收ACK后才更新MaxMessageId
   /** 尽量保证消息最多消费一次，不出现重复消费（注意：只是尽量保证，而非绝对保证。） */
   AT_MOST_ONCE,
   /** 尽量保证消息最少消费一次，不出现消息丢失的情况（注意：只是尽量保证，而非绝对保证。） */
   AT_LEAST_ONCE,
   /** 临时的消费类型，从当前的消息开始消费，不会对消费状态进行持久化，Server重启后将重新开始。 */
   NON_DURABLE

}
