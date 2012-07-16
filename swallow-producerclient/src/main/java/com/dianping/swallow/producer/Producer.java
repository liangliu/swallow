/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-21
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

import com.dianping.swallow.common.producer.exceptions.SendFailedException;

/**
 * Producer接口
 * 
 * @author tong.song
 */
public interface Producer {
   /**
    * @param content 消息体
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws SendFailedException 消息发送失败
    */
   public String sendMessage(Object content) throws SendFailedException;

   /**
    * @param content 消息体
    * @param messageType 用于消息过滤，消息的类型
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws SendFailedException 消息发送失败
    */
   public String sendMessage(Object content, String messageType) throws SendFailedException;

   /**
    * @param content 消息体
    * @param properties 用于消息过滤，留作后用
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws SendFailedException 消息发送失败
    */
   public String sendMessage(Object content, Map<String, String> properties) throws SendFailedException;

   /**
    * @param content 消息体
    * @param properties 用于消息过滤，留作后用
    * @param messageType 用于消息过滤，消息的类型
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws SendFailedException 消息发送失败
    */
   public String sendMessage(Object content, Map<String, String> properties, String messageType)
         throws SendFailedException;
}
