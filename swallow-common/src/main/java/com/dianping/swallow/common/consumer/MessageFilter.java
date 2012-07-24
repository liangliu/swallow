package com.dianping.swallow.common.consumer;

import java.io.Serializable;
import java.util.Set;

public class MessageFilter implements Serializable {
   
   private static final long serialVersionUID = 5643819915814285301L;
   
   public final static MessageFilter AllMatchFilter = new MessageFilter(FilterType.AllMatch, null);

   public enum FilterType {AllMatch, InSet};
   
   private FilterType type;
   private Set<String> param;
   
   private MessageFilter() {
   }
   
   private MessageFilter(FilterType type, Set<String> param) {
      this.type = type;
      this.param = param;
   }
   
   public static MessageFilter createInSetMessageFilter(Set<String> matchTypeSet) {
      return new MessageFilter(FilterType.InSet, matchTypeSet);
   }

   public FilterType getType() {
      return type;
   }

   public Set<String> getParam() {
      return param;
   }

   @Override
   public String toString() {
      return "MessageFilter [param=" + param + ", type=" + type + "]";
   }
   
}
