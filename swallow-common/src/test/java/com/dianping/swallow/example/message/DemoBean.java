package com.dianping.swallow.example.message;

public class DemoBean {

   private int    a;
   private String b;

   public DemoBean() {
      super();
   }

   public int getA() {
      return a;
   }

   public void setA(int a) {
      this.a = a;
   }

   public String getB() {
      return b;
   }

   public void setB(String b) {
      this.b = b;
   }

   @Override
   public String toString() {
      return String.format("DemoBean [a=%s, b=%s]", a, b);
   }

}
