package com.dianping.swallow.common.util;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHAUtil {
   //根据Object生成SHA-1字符串
   public static String generateSHA(Object obj) throws Exception {
      String ret = null;
      ret = generateSHA(objectToByte(obj));
      return ret;
   }

   //根据String生成SHA-1字符串
   public static String generateSHA(String str) {
      return generateSHA(str.getBytes());
   }

   //根据bytes生成SHA-1字符串
   public static String generateSHA(byte[] bytes) {
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

   //将bytes转化为String
   private static String byteToString(byte[] digest) {
      String str = "";
      String tempStr = "";
      for (int i = 1; i < digest.length; i++) {
         tempStr = (Integer.toHexString(digest[i] & 0xff));
         if (tempStr.length() == 1) {
            str = str + "0" + tempStr;
         } else {
            str = str + tempStr;
         }
      }
      return str.toLowerCase();
   }

   //将object转变为bytes
   private static byte[] objectToByte(Object obj) throws Exception {
      if (obj == null)
         return null;
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(bo);
      oo.writeObject(obj);
      return bo.toByteArray();
   }
}
