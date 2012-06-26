/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-7-29
 * $Id$
 * 
 * Copyright 2011 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow;

import java.util.Map;

/***
 * MQ服务接口
 * 
 * @author qing.gu
 * 
 */
public interface MQService {

	/**
	 * consumer参数key
	 * 
	 * @author marsqing
	 * 
	 */
	enum ConsumerOptionKey {

		/**
		 * 使用Topic的Durable Subscriber时，需要色设置全局唯一的ConsumerID，类型为String
		 */
		ConsumerID,

		/**
		 * 使用Queue的Master-Slave Consumer模式时，Slave需要将IsSlaveConsumer设置为true
		 */
		IsSlaveConsumer,

		/**
		 * Consumer消费完消息后是否需要将消息从MQ中删除，默认为true
		 */
		ShouldRemoveMessage,
		
		/**
		 * 是否禁止backout consumer线程，默认为false
		 */
		DisableBackoutConsumer
	};

	/**
	 * producer参数key
	 * 
	 * @author marsqing
	 * 
	 */
	enum ProducerOptionKey {
		/**
		 * 发送消息时如果MQ发生异常，Producer需要重试的次数，默认为0，即不重试，类型为int，设为负值时表示重试直到成功
		 */
		MsgSendRetryCount
	}

	int PRODUCR_RETRY_COUNT_FOREVER = -1;
	int PRODUCR_RETRY_COUNT_NORETRY = 0;

	/**
	 * 创建Producer
	 * 
	 * @param dest
	 *            消息地址
	 * @param options
	 *            附加参数
	 * @return
	 */
	MessageProducer createProducer(Destination dest, Map<ProducerOptionKey, Object> options);
	MessageProducer createProducer(Destination dest);

	/**
	 * 创建Consumer
	 * 
	 * @param dest
	 *            消息地址
	 * @param options
	 *            附加参数
	 * @return
	 */
	MessageConsumer createConsumer(Destination dest, Map<ConsumerOptionKey, Object> options);
	MessageConsumer createConsumer(Destination dest);

	/**
	 * 关闭MQ
	 */
	void close();

}
