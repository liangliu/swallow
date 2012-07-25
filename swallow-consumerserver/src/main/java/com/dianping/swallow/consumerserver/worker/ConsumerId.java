package com.dianping.swallow.consumerserver.worker;

import com.dianping.swallow.common.message.Destination;

public class ConsumerId {

   private String      consumerId;
   private Destination dest;

   public ConsumerId(String consumerId, Destination dest) {
      super();
      this.consumerId = consumerId;
      this.dest = dest;
   }

   public String getConsumerId() {
      return consumerId;
   }

   public Destination getDest() {
      return dest;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((consumerId == null) ? 0 : consumerId.hashCode());
      result = prime * result + ((dest == null) ? 0 : dest.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (getClass() != obj.getClass()) {
         return false;
      }
      ConsumerId other = (ConsumerId) obj;
      if (consumerId == null) {
         if (other.consumerId != null) {
            return false;
         }
      } else if (!consumerId.equals(other.consumerId)) {
         return false;
      }
      if (dest == null) {
         if (other.dest != null) {
            return false;
         }
      } else if (!dest.equals(other.dest)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      return String.format("ConsumerId [consumerId=%s, dest=%s]", consumerId, dest);
   }

}
