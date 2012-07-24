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
package com.dianping.swallow.common.internal.threadfactory;

import org.apache.log4j.Logger;

/**
 * 
 * 失败时累加failCnt，sleep(min(delayBase * failCnt, delayUpperbound))，成功时清零failCnt
 * 
 * @author marsqing
 * 
 */
public class DefaultPullStrategy implements PullStrategy {
	
	private static Logger log = Logger.getLogger(DefaultPullStrategy.class);

	private int failCnt = 0;
	private final int delayBase;
	private final int delayUpperbound;

	public DefaultPullStrategy(int delayBase, int delayUpperbound) {
		this.delayBase = delayBase;
		this.delayUpperbound = delayUpperbound;
	}

	@Override
	public long fail(boolean shouldSleep) throws InterruptedException {
		failCnt++;
		long sleepTime = (long)failCnt * delayBase;
		sleepTime = sleepTime > delayUpperbound ? delayUpperbound : sleepTime;
		if(shouldSleep) {
		   if(log.isDebugEnabled()) {
	         log.debug("sleep " + sleepTime + " at " + this.getClass().getSimpleName());
	      }
		   Thread.sleep(sleepTime);
		}
		return sleepTime;
	}

	@Override
	public void succeess() {
		failCnt = 0;
	}

}
