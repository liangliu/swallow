/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-25
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.producer;

import java.util.Map;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.impl.ProducerImpl;

/**
 * Producer工厂类接口
 * 
 * @author tong.song
 */
public interface ProducerFactory {
   /**
    * 获取指定配置的Producer实例
    * 
    * @param topicName Producer消息目的地，类型为{@link Destination}
    * @param pOptions Producer选项，如果需要与默认配置不同的选项，则将其加入map
    * @return ProducerImpl对象，用于发送消息
    * @throws TopicNameInvalidException 目的地名称非法，则抛出异常
    * @throws RemoteServiceDownException Producer尝试连接远程服务失败时，抛出此异常
    */
   public ProducerImpl getProducer(Destination topicName, Map<ProducerOptionKey, Object> pOptions)
         throws TopicNameInvalidException, RemoteServiceDownException;
   /**
    * 获取默认配置的Producer实例
    * @param topicName Producer消息目的地，类型为{@link Destination}
    * @return ProducerImpl对象，用于发送消息
    * @throws TopicNameInvalidException 目的地名称非法，则抛出异常
    * @throws RemoteServiceDownException Producer尝试连接远程服务失败时，抛出此异常
    */
   public ProducerImpl getProducer(Destination topicName) throws TopicNameInvalidException, RemoteServiceDownException;
}
