package com.dianping.swallow.producerserver.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHAGenerater {
	
	public static String generateSHA(String str) {
		return generateSHA(str.getBytes());
	}
	
	public static String generateSHA(byte[] bytes){
		String ret = null;
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			byte[] strDigest = md.digest(bytes);
			ret = byteToString(strDigest);
		} catch (NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
		}
		return ret;
	}
	
	private static String byteToString(byte[] digest) {
		String str = "";
		String tempStr = "";
		for (int i = 1; i < digest.length; i++) {
			tempStr = (Integer.toHexString(digest[i] & 0xff));
			if (tempStr.length() == 1) {
				str = str + "0" + tempStr;
			}else {
				str = str + tempStr;
			}
		}
		return str.toLowerCase();
	}
}
