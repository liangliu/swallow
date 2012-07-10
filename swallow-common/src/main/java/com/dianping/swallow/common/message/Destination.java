/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-7-29
 * $Id$
 * 
 * Copyright 2011 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.common.message;

import java.io.Serializable;

/***
 * 消息地址
 * 
 * @author qing.gu
 */
public class Destination implements Serializable {

   private static final long serialVersionUID = 3571573051999434062L;

   private String            name;
   private Type              type;

   private enum Type {
      QUEUE,
      TOPIC
   };

   private Destination() {
   }

   private Destination(String name, Type type) {
      this.name = name;
      this.type = type;
   }

   // 此版本不实现queue类型
   //   /***
   //    * 创建Queue类型地址
   //    * 
   //    * @param name Queue名称
   //    * @return
   //    */
   //   public static Destination queue(String name) {
   //      return new Destination(name, Type.QUEUE);
   //   }

   /***
    * 创建Topic类型地址
    * 
    * @param name Topic名称
    * @return
    */
   public static Destination topic(String name) {
      return new Destination(name, Type.TOPIC);
   }

   public String getName() {
      return name;
   }

   // 此版本不实现queue类型，故不需要以下2个方法
   //   public boolean isQueue() {
   //      return type == Type.QUEUE;
   //   }
   //
   //   public boolean isTopic() {
   //      return type == Type.TOPIC;
   //   }

   @Override
   public String toString() {
      return String.format("Destination [name=%s, type=%s]", name, type);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Destination other = (Destination) obj;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (type != other.type)
         return false;
      return true;
   }

}
