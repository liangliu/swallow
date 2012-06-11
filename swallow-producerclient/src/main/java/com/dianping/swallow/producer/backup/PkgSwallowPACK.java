/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-25
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
package com.dianping.swallow.producer.backup;

/**
 * TODO Comment of PktSwallowPACK
 * @author tong.song
 *
 */
public final class PkgSwallowPACK extends Package {
	private int SEQ;

	public void setSEQ(int SEQ) {
		this.SEQ = SEQ;
	}
	public int getSEQ() {
		return SEQ;
	}
	public PkgSwallowPACK(int SEQ){
		this.setPackageType(PackageType.SWALLOW_P_ACK);
		this.SEQ = SEQ;
	}
}
