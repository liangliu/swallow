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

import com.dianping.swallow.common.producer.exceptions.TopicNameInvalidException;
import com.dianping.swallow.producer.impl.ProducerImpl;

/**
 * Producer工厂类接口
 * @author tong.song
 *
 */
public interface ProducerFactory {
   public ProducerImpl getProducer(String topicName, Map<ProducerOptionKey, Object> pOptions) throws TopicNameInvalidException;
}
