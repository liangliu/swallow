package com.dianping.swallow.common.internal.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ZipUtil {
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
      return out.toString("ISO-8859-1");
   }

   /**
    * 解压缩字符串
    * 
    * @param str 符合gzip格式的、压缩后的字符串（由zip函数压缩过的字符串）
    * @return 解压缩后的字符串
    * @throws IOException IO处理出错
    */
   public static String unzip(String str) throws IOException {
      if (str == null || str.length() == 0)
         return str;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));
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
