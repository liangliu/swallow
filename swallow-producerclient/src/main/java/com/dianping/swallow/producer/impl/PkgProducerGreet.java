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
package com.dianping.swallow.producer.impl;



/**
 * TODO Comment of PktProducerGreet
 * @author tong.song
 *
 */
public final class PkgProducerGreet extends Package {
	private String producerVersion;
	public PkgProducerGreet(String producerVersion){
		this.setPackageType(PackageType.PRODUCER_GREET);
		this.setProducerVersion(producerVersion);
	}
	private void setProducerVersion(String producerVersion) {
		this.producerVersion = producerVersion;
	}
	public String getProducerVersion() {
		return producerVersion;
	}
}