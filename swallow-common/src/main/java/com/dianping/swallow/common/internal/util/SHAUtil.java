package com.dianping.swallow.common.internal.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHAUtil {
   private SHAUtil() {
   }

   //根据String生成SHA-1字符串
   public static String generateSHA(String str) {
      try {
         return generateSHA(str.getBytes("UTF-8"));
      } catch (UnsupportedEncodingException uee) {
         return null;
      }
   }

   //根据bytes生成SHA-1字符串
   private static String generateSHA(byte[] bytes) {
      String ret = null;
      try {
         MessageDigest md = MessageDigest.getInstance("SHA-1");
         byte[] byteDigest = md.digest(bytes);
         ret = byteToString(byteDigest);
      } catch (NoSuchAlgorithmException nsae) {
         nsae.printStackTrace();
      }
      return ret;
   }

   //将bytes转化为String
   private static String byteToString(byte[] digest) {
      String tmpStr = "";
      StringBuffer strBuf = new StringBuffer(40);
      for (int i = 0; i < digest.length; i++) {
         tmpStr = (Integer.toHexString(digest[i] & 0xff));
         if (tmpStr.length() == 1) {
            strBuf.append("0" + tmpStr);
         } else {
            strBuf.append(tmpStr);
         }
      }
      return strBuf.toString();
   }
}
