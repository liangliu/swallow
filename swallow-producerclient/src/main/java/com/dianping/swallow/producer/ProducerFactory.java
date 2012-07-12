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

import com.dianping.swallow.common.internal.producer.ProducerSwallowService;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;

/**
 * Producer工厂类接口
 * 
 * @author tong.song
 */
public interface ProducerFactory {
   /**
    * @return 获取当前可以生成的Producer的版本号
    */
   public String getProducerVersion();

   /**
    * @return 获取当前Producer的本机IP地址
    */
   public String getProducerIP();

   /**
    * @return 获取远程调用
    */
   public ProducerSwallowService getRemoteService();

   /**
    * 获取默认配置的Producer实例
    * 
    * @param dest Producer消息目的地，类型为{@link Destination}
    * @return 实现Producer接口的对象，用于发送消息，此版本中类型为{@link ProducerImpl}
    * @throws TopicNameInvalidException 目的地名称非法，则抛出异常
    */
   public Producer getProducer(Destination dest) throws TopicNameInvalidException;

   /**
    * 获取指定配置的Producer实例
    * 
    * @param dest Producer消息目的地，类型为{@link Destination}
    * @param pOptions Producer选项，如果需要与默认配置不同的选项，则将其加入map
    * @return 实现Producer接口的对象，用于发送消息，此版本中类型为{@link ProducerImpl}
    * @throws TopicNameInvalidException 目的地名称非法，则抛出异常
    */
   public Producer getProducer(Destination dest, Map<ProducerOptionKey, Object> pOptions)
         throws TopicNameInvalidException;
}
