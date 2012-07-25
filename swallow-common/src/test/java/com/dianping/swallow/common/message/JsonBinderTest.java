package com.dianping.swallow.common.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.dianping.swallow.common.internal.codec.JsonBinder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 测试Jackson对Object,Map,List,数组,枚举,日期类等的持久化.
 */
public class JsonBinderTest {

   private static JsonBinder binder = JsonBinder.getNonEmptyBinder();

   /**
    * 序列化对象/集合到Json字符串.
    */
   @Test
   public void toJson() throws Exception {
      //Bean
      TestBean bean = new TestBean("A");
      String beanString = binder.toJson(bean);
      assertEquals("{\"name\":\"A\",\"defaultValue\":\"hello\"}", beanString);

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

      //Empty bean
      EmptyBean bean = new EmptyBean();
      Assert.assertNotNull(binder.toJson(bean));
      Assert.assertEquals("{}", binder.toJson(bean));

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
      Assert.assertNotNull(binder.fromJson("{}", EmptyBean.class));

      nullListResult = binder.fromJson("null", List.class);
      assertNull(nullListResult);

      nullListResult = binder.fromJson("[]", List.class);
      assertEquals(0, nullListResult.size());
   }

   @Test
   public void error() {
      JsonBinder binder = JsonBinder.getNonEmptyBinder();
      // error json string
      try {
         binder.fromJson("error json string", EmptyBean.class);
         Assert.fail();
      } catch (Exception e) {
         Assert.assertTrue(e instanceof JsonDeserializedException);
      }

   }

   public static class EmptyBean {
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
         if (this == obj) {
            return true;
         }
         if (obj == null) {
            return false;
         }
         if (!(obj instanceof TestBean)) {
            return false;
         }
         TestBean other = (TestBean) obj;
         if (defaultValue == null) {
            if (other.defaultValue != null) {
               return false;
            }
         } else if (!defaultValue.equals(other.defaultValue)) {
            return false;
         }
         if (name == null) {
            if (other.name != null) {
               return false;
            }
         } else if (!name.equals(other.name)) {
            return false;
         }
         if (nullValue == null) {
            if (other.nullValue != null) {
               return false;
            }
         } else if (!nullValue.equals(other.nullValue)) {
            return false;
         }
         return true;
      }
   }

}
