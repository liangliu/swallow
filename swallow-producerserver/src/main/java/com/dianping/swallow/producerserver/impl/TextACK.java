/**
 * Project: swallow-producerserver
 * 
 * File Created at 2012-6-26
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
 * TODO Comment of TextACK
 * 
 * @author tong.song
 */
public class TextACK {
   private int    status; //状态代码
   private String info;  //详细信息

   @Override
   public String toString() {
      return "status=" + getStatus() + "; info=" + getInfo();
   }

   public int getStatus() {
      return status;
   }

   public void setStatus(int status) {
      this.status = status;
   }

   public String getInfo() {
      return info;
   }

   public void setInfo(String info) {
      this.info = info;
   }
}
