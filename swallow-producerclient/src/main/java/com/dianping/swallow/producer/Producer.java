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

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

/**
 * Producer接口
 * 
 * @author tong.song
 */
public interface Producer {
   /**
    * @param content 消息体
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws FileQueueClosedException 异步：保存至filequeue失败
    * @throws ServerDaoException 同步：远程数据库写入失败
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   public String sendMessage(Object content) throws ServerDaoException, FileQueueClosedException,
         RemoteServiceDownException, NullContentException;

   /**
    * @param content 消息体
    * @param messageType 用于消息过滤，消息的类型
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws FileQueueClosedException 异步：保存至filequeue失败
    * @throws ServerDaoException 同步：远程数据库写入失败
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   public String sendMessage(Object content, String messageType) throws ServerDaoException, FileQueueClosedException,
         RemoteServiceDownException, NullContentException;

   /**
    * @param content 消息体
    * @param properties 用于消息过滤，留作后用
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws FileQueueClosedException 异步：保存至filequeue失败
    * @throws ServerDaoException 同步：远程数据库写入失败
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   public String sendMessage(Object content, Map<String, String> properties) throws ServerDaoException,
         FileQueueClosedException, RemoteServiceDownException, NullContentException;

   /**
    * @param content 消息体
    * @param properties 用于消息过滤，留作后用
    * @param messageType 用于消息过滤，消息的类型
    * @return 同步：content转化为json字符串后的SHA-1签名，异步：null
    * @throws FileQueueClosedException 异步：保存至filequeue失败
    * @throws ServerDaoException 同步：远程数据库写入失败
    * @throws NullContentException 如果待发送的消息content为空指针，则抛出该异常
    */
   public String sendMessage(Object content, Map<String, String> properties, String messageType)
         throws ServerDaoException, FileQueueClosedException, RemoteServiceDownException, NullContentException;
}
