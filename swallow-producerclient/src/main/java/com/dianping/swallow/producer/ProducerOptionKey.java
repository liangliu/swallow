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

/**
 * Producer选项
 * @author tong.song
 *
 */
public enum ProducerOptionKey {
   /**
    * Producer工作模式：同步/异步；前缀为ASYNC_的选项：异步模式生效，同步模式忽略
    */
   PRODUCER_MODE,
   /**
    * 异步模式时，从队列中获取并发送消息的线程池大小
    */
   ASYNC_THREAD_POOL_SIZE,
   /**
    * 异步模式时，重启Producer是否续接上次的队列，继续发送
    */
   ASYNC_IS_CONTINUE_SEND,
   /**
    * 异步模式时，线程池从队列获取并发送消息的失败重试次数
    */
   ASYNC_RETRY_TIMES
   
}
