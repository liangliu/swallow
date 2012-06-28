package com.dianping.swallow.common.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 测试Jackson对Object,Map,List,数组,枚举,日期类等的持久化.
 */
public class JsonBinderTest {

   private static JsonBinder binder = JsonBinder.buildNormalBinder();

   /**
    * 序列化对象/集合到Json字符串.
    */
   @Test
   public void toJson() throws Exception {
      //Bean
      TestBean bean = new TestBean("A");
      String beanString = binder.toJson(bean);
      assertEquals("{\"name\":\"A\",\"defaultValue\":\"hello\",\"nullValue\":null}", beanString);

      //Map
      Map<String, Object> map = Maps.newLinkedHashMap();
      map.put("name", "A");
      map.put("age", 2);
      String mapString = binder.toJson(map);
      assertEquals("{\"name\":\"A\",\"age\":2}", mapString);

      //List<String>
      List<String> stringList = Lists.newArrayList("A", "B", "C");
      String listString = binder.toJson(stringList);
      assertEquals("[\"A\",\"B\",\"C\"]", listString);

   }

   /**
    * 从Json字符串反序列化对象/集合.
    */
   @Test
   public void fromJson() throws Exception {
      //Bean
      String beanString = "{\"name\":\"A\"}";
      TestBean bean = binder.fromJson(beanString, TestBean.class);
      assertEquals(new TestBean("A"), bean);

   }

   /**
    * 测试传入空对象,空字符串,Empty的集合,"null"字符串的结果.
    */
   @SuppressWarnings("rawtypes")
   @Test
   public void nullAndEmpty() {
      // toJson测试 //

      //Null Bean
      TestBean nullBean = null;
      String nullBeanString = binder.toJson(nullBean);
      assertEquals("null", nullBeanString);

      //Empty List
      List<String> emptyList = Lists.newArrayList();
      String emptyListString = binder.toJson(emptyList);
      assertEquals("[]", emptyListString);

      // fromJson测试 //

      //Null String for Bean
      TestBean nullBeanResult = binder.fromJson(null, TestBean.class);
      assertNull(nullBeanResult);

      nullBeanResult = binder.fromJson("null", TestBean.class);
      assertNull(nullBeanResult);

      //Null/Empty String for List
      List nullListResult = binder.fromJson(null, List.class);
      assertNull(nullListResult);

      nullListResult = binder.fromJson("null", List.class);
      assertNull(nullListResult);

      nullListResult = binder.fromJson("[]", List.class);
      assertEquals(0, nullListResult.size());
   }

   /**
    * 测试三种不同的Binder.
    */
   @Test
   public void threeTypeBinders() {
      //打印全部属性
      JsonBinder normalBinder = JsonBinder.buildNormalBinder();
      TestBean bean = new TestBean("A");
      assertEquals("{\"name\":\"A\",\"defaultValue\":\"hello\",\"nullValue\":null}", normalBinder.toJson(bean));

   }

   @Test
   public void error() {
      JsonBinder normalBinder = JsonBinder.buildNormalBinder();
      ErrorBean bean = new ErrorBean();
      assertNull(normalBinder.toJson(bean));
      assertNull(normalBinder.fromJson("error json string", ErrorBean.class));
   }

   public static class ErrorBean {
      int getA() {
         return 5;
      }
   }

   public static class TestBean {

      private String name;
      private String defaultValue = "hello";
      private String nullValue    = null;

      public TestBean() {
      }

      public TestBean(String name) {
         this.name = name;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getDefaultValue() {
         return defaultValue;
      }

      public void setDefaultValue(String defaultValue) {
         this.defaultValue = defaultValue;
      }

      public String getNullValue() {
         return nullValue;
      }

      public void setNullValue(String nullValue) {
         this.nullValue = nullValue;
      }

      @Override
      public String toString() {
         return "TestBean [defaultValue=" + defaultValue + ", name=" + name + ", nullValue=" + nullValue + "]";
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
         result = prime * result + ((name == null) ? 0 : name.hashCode());
         result = prime * result + ((nullValue == null) ? 0 : nullValue.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (!(obj instanceof TestBean))
            return false;
         TestBean other = (TestBean) obj;
         if (defaultValue == null) {
            if (other.defaultValue != null)
               return false;
         } else if (!defaultValue.equals(other.defaultValue))
            return false;
         if (name == null) {
            if (other.name != null)
               return false;
         } else if (!name.equals(other.name))
            return false;
         if (nullValue == null) {
            if (other.nullValue != null)
               return false;
         } else if (!nullValue.equals(other.nullValue))
            return false;
         return true;
      }
   }

}
