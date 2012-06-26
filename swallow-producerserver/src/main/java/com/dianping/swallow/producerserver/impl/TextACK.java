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
   private boolean isOK;  //是否需要ACK
   private String  sha1;  //Message内容的sha1签名
   private String  reason; //保存失败的原因

   public boolean isOK() {
      return isOK;
   }

   public void setOK(boolean isOK) {
      this.isOK = isOK;
   }

   public String getSha1() {
      return sha1;
   }

   public void setSha1(String sha1) {
      this.sha1 = sha1;
   }

   public String getReason() {
      return reason;
   }

   public void setReason(String reason) {
      this.reason = reason;
   }

   @Override
   public String toString() {
      return "isOK=" + isOK + ";\tSHA-1=" + sha1 + ";\treason=" + reason + ";";
   }
}