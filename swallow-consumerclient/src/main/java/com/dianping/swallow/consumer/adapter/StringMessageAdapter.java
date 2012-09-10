package com.dianping.swallow.consumer.adapter;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dianping.swallow.StringMessage;
import com.dianping.swallow.common.message.Message;

public class StringMessageAdapter implements StringMessage {

   private Message newMsg;

   public StringMessageAdapter(Message newMsg) {
      this.newMsg = newMsg;
   }

   @Override
   public Date getAddtime() {
      return newMsg.getGeneratedTime();
   }

   @Override
   public void setProperty(String name, String value) {
   }

   @Override
   public String getProperty(String name) {
      Map<String, String> properties = newMsg.getProperties();
      String value = null;
      if (properties != null) {
         value = properties.get(name);
      }
      return value;
   }

   @Override
   public Set<String> getPropertyNames() {
      Map<String, String> properties = newMsg.getProperties();
      Set<String> names = new HashSet<String>();
      if (properties != null) {
         names.addAll(properties.keySet());
      }
      return names;
   }

   @Override
   public boolean isBackout() {
      return false;
   }

   @Override
   public int getBackoutCnt() {
      return 0;
   }

   @Override
   public String getBackoutReason() {
      return "NOT SUPPORTED AFTER SWALLOW 0.6";
   }

   @Override
   public Date getBackoutOriginalAddtime() {
      return newMsg.getGeneratedTime();
   }

   @Override
   public String getBackoutStackTrace() {
      return "NOT SUPPORTED AFTER SWALLOW 0.6";
   }

   @Override
   public String getContent() {
      return newMsg.getContent();
   }

}
