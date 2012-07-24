package com.dianping.swallow.example.message;

public class DemoBean {

   private int    a;
   private String b;

   public DemoBean() {
      super();
   }

   public void setA(int a) {
      this.a = a;
   }

   public void setB(String b) {
      this.b = b;
   }

   @Override
   public String toString() {
      return String.format("DemoBean [a=%s, b=%s]", a, b);
   }

}
