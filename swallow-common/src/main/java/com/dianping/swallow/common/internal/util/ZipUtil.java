package com.dianping.swallow.common.internal.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

public class ZipUtil {
   private ZipUtil() {
   }

   /**
    * 压缩字符串
    * 
    * @param str 待压缩的字符串
    * @return 压缩后的字符串，可能不可显示或显示为乱码
    * @throws IOException IO处理出错
    */
   public static String zip(String str) throws IOException {
      if (str == null || str.length() == 0)
         return str;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      GZIPOutputStream gzip = new GZIPOutputStream(out);
      try {
         gzip.write(str.getBytes("UTF-8"));
      } finally {
         gzip.close();
      }
      return Base64.encodeBase64String(out.toByteArray());
   }

   /**
    * 解压缩字符串
    * 
    * @param str 符合gzip格式的、压缩后的字符串（由zip函数压缩过的字符串）
    * @return 解压缩后的字符串
    * @throws IOException IO处理出错
    */
   public static String unzip(String zipedBase64String) throws IOException {
      if (zipedBase64String == null || zipedBase64String.length() == 0)
         return zipedBase64String;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteArrayInputStream in = new ByteArrayInputStream(Base64.decodeBase64(zipedBase64String));
      GZIPInputStream gunzip = new GZIPInputStream(in);
      try {
         byte[] buffer = new byte[256];
         int n;
         while ((n = gunzip.read(buffer)) != -1) {
            out.write(buffer, 0, n);
         }
      } finally {
         gunzip.close();
      }
      return out.toString("UTF-8");
   }

   public static void main(String[] args) throws IOException {
      System.out.println(zip("测试中文Encoding。"));
      System.out.println(unzip(zip("测试中文Encoding。")));
   }

}
