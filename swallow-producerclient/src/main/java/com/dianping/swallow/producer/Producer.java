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
    * Swallow发送端对content进行json序列化成字符串进行传输，在接收端可以接收到json，并且可以通过<code>Message.transferContentToBean</code>将json反序列化成Object。<br>
    * <p>
    * 开发时需要注意以下2点：<br>
    * (1)请确保content对象的类型具有默认构造方法。<br>
    * (2)尽量保证content对象是简单的类型(如String/基本类型包装类/POJO)。如果content是复杂的类型，建议在您的项目上线之前，在接收消息端做测试，验证是否能够将content正常反序列化(调用反序列化方法<code>Message.transferContentToBean()</code>)。
    * </p>
    * 
    * @param content 待发送的消息内容，类型为{@link Object}，不能为null，否则抛出
    *           {@link IllegalArgumentException}
    * @return 同步模式：content转化为json字符串后的SHA-1签名，异步模式：null
    * @throws SendFailedException 消息发送失败，可能的原因包括：网络、数据库及FileQueue故障
    */
   String sendMessage(Object content) throws SendFailedException;

   /**
    * Swallow发送端对content进行json序列化成字符串进行传输，在接收端可以接收到json，并且可以通过<code>Message.transferContentToBean</code>将json反序列化成Object。<br>
    * <p>
    * 开发时需要注意以下2点：<br>
    * (1)请确保content对象的类型具有默认构造方法。<br>
    * (2)尽量保证content对象是简单的类型(如String/基本类型包装类/POJO)。如果content是复杂的类型，建议在您的项目上线之前，在接收消息端做测试，验证是否能够将content正常反序列化(调用反序列化方法<code>Message.transferContentToBean()</code>)。
    * </p>
    * 
    * @param content 待发送的消息内容，类型为{@link Object}，不能为null，否则抛出
    *           {@link IllegalArgumentException}
    * @param messageType 消息类型，用于过滤指定类型的消息
    * @return 同步模式：content转化为json字符串后的SHA-1签名，异步模式：null
    * @throws SendFailedException 消息发送失败，可能的原因包括：网络、数据库及FileQueue故障
    */
   String sendMessage(Object content, String messageType) throws SendFailedException;

   /**
    * Swallow发送端对content进行json序列化成字符串进行传输，在接收端可以接收到json，并且可以通过<code>Message.transferContentToBean</code>将json反序列化成Object。<br>
    * <p>
    * 开发时需要注意以下2点：<br>
    * (1)请确保content对象的类型具有默认构造方法。<br>
    * (2)尽量保证content对象是简单的类型(如String/基本类型包装类/POJO)。如果content是复杂的类型，建议在您的项目上线之前，在接收消息端做测试，验证是否能够将content正常反序列化(调用反序列化方法<code>Message.transferContentToBean()</code>)。
    * </p>
    * 
    * @param content 待发送的消息内容，类型为{@link Object}，不能为null，否则抛出
    *           {@link IllegalArgumentException}
    * @param properties 消息属性，Key和Value类型非法时抛出{@link IllegalArgumentException}
    *           ，Value可以为null
    * @return 同步模式：content转化为json字符串后的SHA-1签名，异步模式：null
    * @throws SendFailedException 消息发送失败，可能的原因包括：网络、数据库及FileQueue故障
    */
   String sendMessage(Object content, Map<String, String> properties) throws SendFailedException;

   /**
    * Swallow发送端对content进行json序列化成字符串进行传输，在接收端可以接收到json，并且可以通过<code>Message.transferContentToBean</code>将json反序列化成Object。<br>
    * <p>
    * 开发时需要注意以下2点：<br>
    * (1)请确保content对象的类型具有默认构造方法。<br>
    * (2)尽量保证content对象是简单的类型(如String/基本类型包装类/POJO)。如果content是复杂的类型，建议在您的项目上线之前，在接收消息端做测试，验证是否能够将content正常反序列化(调用反序列化方法<code>Message.transferContentToBean()</code>)。
    * </p>
    * 
    * @param content 待发送的消息内容，类型为{@link Object}，不能为null，否则抛出
    *           {@link IllegalArgumentException}
    * @param properties 消息属性，Key和Value类型非法时抛出{@link IllegalArgumentException}
    *           ，Value可以为null
    * @param messageType 消息类型，用于过滤指定类型的消息
    * @return 同步模式：content转化为json字符串后的SHA-1签名，异步模式：null
    * @throws SendFailedException 消息发送失败，可能的原因包括：网络、数据库及FileQueue故障
    */
   String sendMessage(Object content, Map<String, String> properties, String messageType) throws SendFailedException;
}
