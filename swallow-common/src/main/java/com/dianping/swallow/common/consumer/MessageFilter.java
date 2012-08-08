package com.dianping.swallow.common.consumer;

import java.io.Serializable;
import java.util.Set;

/**
 * 该类用于作消息过滤。<br>
 * 使用<code>createInSetMessageFilter(Set&lt;String&gt; matchTypeSet)</code>
 * 构造实例时，参数matchTypeSet指定了Consumer只消费“Message.type属性包含在matchTypeSet中”的消息
 * 
 * @author kezhu.wu
 */
public final class MessageFilter implements Serializable {

   private static final long         serialVersionUID = 5643819915814285301L;

   public final static MessageFilter AllMatchFilter   = new MessageFilter(FilterType.AllMatch, null);

   public enum FilterType {
      AllMatch,
      InSet
   };

   private FilterType  type;
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
