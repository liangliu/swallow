/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-7-29
 * $Id$
 * 
 * Copyright 2011 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow;
/***
 * 
 * @author marsqing
 *
 */
public class MQException extends Exception {

   private static final long serialVersionUID = 1005746850003686607L;

   public MQException() {
		super();
	}

	public MQException(String message, Throwable cause) {
		super(message, cause);
	}

	public MQException(String message) {
		super(message);
	}

	public MQException(Throwable cause) {
		super(cause);
	}

}
