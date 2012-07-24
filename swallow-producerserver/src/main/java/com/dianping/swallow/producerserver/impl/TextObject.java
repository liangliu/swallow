/**
 * Project: swallow-producerserver
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
package com.dianping.swallow.producerserver.impl;

/**
 * 可以处理的文本流格式，需要包含topic、content、isACK三方面内容
 * 
 * @author tong.song
 */
public class TextObject {
   private String  topic;  //TextMessage的TopicName
   private String  content; //TextMessage的内容
   private boolean isACK;  //是否需要ACK

   public String getTopic() {
      return topic;
   }

   public void setTopic(String topic) {
      this.topic = topic;
   }

   public String getContent() {
      return content;
   }

   public void setContent(String content) {
      this.content = content;
   }

   public boolean isACK() {
      return isACK;
   }

   public void setACK(boolean isACK) {
      this.isACK = isACK;
   }
   
   public boolean getIsACK() {
	   return isACK;
   }

   @Override
   public String toString() {
      return "topic=" + getTopic() + ";\tcontent=" + getContent() + ";\tisACK=" + isACK() + ";";
   }
}