package com.dianping.swallow.producer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public final class ProducerConfigure {
	private int				senderNum		= 10;
	private String			filequeueName	= null;
	private ProducerMode	producerMode	= ProducerMode.SYNCHRO;//同步模式
	
	
	private static final Logger 	log		= Logger.getLogger(ProducerConfigure.class);
	
	private ProducerConfigure(String configFile){
		Properties prop = new Properties();
		InputStream in = null;
		try {
			in = ProducerConfigure.class.getClassLoader().getResourceAsStream(configFile);
			prop.load(in);
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Can't load Configure File.");
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					System.out.println("Close inputstream wrong.");
				}
			}

		}
	}
	public static enum ProducerMode{
		SYNCHRO,
		ASYNCHRO
	}

	public int getSenderNum() {
		return senderNum;
	}

	public void setSenderNum(int senderNum) {
		this.senderNum = senderNum;
	}

	public String getFilequeueName() {
		return filequeueName;
	}

	public void setFilequeueName(String filequeueName) {
		this.filequeueName = filequeueName;
	}

	public ProducerMode getProducerMode() {
		return producerMode;
	}

	public void setProducerMode(ProducerMode producerMode) {
		this.producerMode = producerMode;
	}
}
