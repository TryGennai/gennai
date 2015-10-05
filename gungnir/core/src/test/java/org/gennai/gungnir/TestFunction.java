/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir;

import static org.gennai.gungnir.ql.QueryOperations.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.guava.collect.Lists;
import org.gennai.gungnir.ql.FunctionValidateException;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestFunction {

  public static class TestFunc {

    public String evaluate(int v1) {
      return "i:" + v1;
    }

    public String evaluate(Object v1) {
      return "O:" + v1;
    }

    public String evaluate(Integer v1) {
      return "I:" + v1;
    }
  }

  public static class TestFunc2 {

    public String evaluate(int v1) {
      return "i:" + v1;
    }

    public String evaluate(Object v1) {
      return "O:" + v1;
    }
  }

  public static class TestFunc3 {

    public String evaluate(Object v1) {
      return "O:" + v1;
    }
  }

  public static class TestFunc4 {

    private String evaluate(int v1) {
      return "i:" + v1;
    }

    public String dummy(int v1) {
      return evaluate(v1);
    }
  }

  public static class TestFunc5 {

    public Object evaluate(Object v1, StringBuilder v2) {
      return v1 + v2.toString();
    }
  }

  public static class TestFunc6 {

    public SimpleDateFormat evaluate(Object v1, String v2) {
      return null;
    }
  }

  public static class TestFunc7 {

    public Map<String, Integer> evaluate(Boolean v1, String v2, long v3) {
      return null;
    }

    public List<String> evaluate(Date v1, boolean v2, short v3) {
      return null;
    }

    public Boolean evaluate(Map<String, Double> v1, List<Long> v2, Struct v3) {
      return false;
    }
  }

  public static class TestFunc8 {

    public String evaluate(int v1) {
      return "i:" + v1;
    }

    public String evaluate(Object v1) {
      return "O:" + v1;
    }

    public String evaluate(Integer v1) {
      return "I:" + v1;
    }

    public String evaluate(Object v1, int v2) {
      return "Oi:" + v1 + "-" + v2;
    }

    public String evaluate(String v1, int v2) {
      return "Si:" + v1 + "-" + v2;
    }

    public String evaluate(String v1, Integer v2) {
      return "SI:" + v1 + "-" + v2;
    }
  }

  public static class TestFunc9 {

    private long total = 0;

    public Long evaluate(Integer v1) {
      total += v1;
      return total;
    }

    public Long exclude(Integer v1) {
      total -= v1;
      return total;
    }

    public void clear() {
      total = 0;
    }
  }

  public static class TestFunc10 {

    private long total = 0;

    public Long evaluate(Integer v1) {
      total += v1;
      return total;
    }

    public void clear() {
      total = 0;
    }
  }

  public static class TestFunc11 {

    private long total = 0;

    public Long evaluate(Integer v1) {
      total += v1;
      return total;
    }

    public Long exclude(Integer v1) {
      total -= v1;
      return total;
    }
  }

  public static class TestFunc12 {

    private long count = 0;

    public Long evaluate() {
      count++;
      return count;
    }

    public Long exclude() {
      count--;
      return count;
    }

    public void clear() {
      count = 0;
    }
  }

  public static class TestFunc13 {

    private long total = 0;

    public Long evaluate(Integer v1) {
      total += v1;
      return total;
    }

    public Long evaluate(Object v1) {
      return total;
    }

    public Long exclude(Object v1) {
      return total;
    }

    public Long exclude(String v1) {
      return total;
    }

    public void clear() {
      total = 0;
    }
  }

  public static class TestFunc14 {

    public String evaluate(String v1) {
      return null;
    }

    public String exclude(StringBuilder v1) {
      return null;
    }

    public void clear() {
    }
  }

  public static class TestFunc15 {

    public String evaluate(String v1) {
      return null;
    }

    public String exclude(String v1) {
      return null;
    }

    public void clear(String v1) {
    }
  }

  public static class TestFunc16 {

    public String evaluate(String v1) {
      return null;
    }

    public BitSet exclude(String v1) {
      return null;
    }

    public void clear() {
    }
  }

  public static class TestFunc17 {

    public String evaluate(String v1) {
      return null;
    }

    public String exclude(String v1) {
      return null;
    }

    public String clear() {
      return null;
    }
  }

  public static class TestFunc18 {

    public Map<String, Integer> evaluate(Boolean v1, String v2, long v3) {
      return null;
    }

    public List<String> evaluate(Date v1, boolean v2, short v3) {
      return null;
    }

    public Boolean evaluate(Map<String, Double> v1, List<Long> v2, Struct v3) {
      return false;
    }

    public Map<String, Integer> exclude(Boolean v1, String v2, long v3) {
      return null;
    }

    public List<String> exclude(Date v1, boolean v2, short v3) {
      return null;
    }

    public Boolean exclude(Map<String, Double> v1, List<Long> v2, Struct v3) {
      return false;
    }

    public void clear() {
    }
  }

  public static class TestFunc19 {

    private long cnt;

    public Long evaluate() {
      cnt++;
      return cnt;
    }

    public Long exclude() {
      cnt--;
      return cnt;
    }

    public Long evaluate(Integer... v) {
      cnt += v.length;
      return cnt;
    }

    public Long exclude(Integer... v) {
      cnt -= v.length;
      return cnt;
    }

    public void clear() {
      cnt = 0;
    }
  }

  public static class TestFunc20 {

    public String evaluate(Integer v) {
      return "I:" + v;
    }

    public String evaluate(int v) {
      return "i:" + v;
    }

    public String evaluate(Object v) {
      return "O:" + v;
    }

    public String evaluate(Integer... v) {
      return "I:" + Arrays.toString(v);
    }

    public String evaluate(int... v) {
      return "i:" + Arrays.toString(v);
    }

    public String evaluate(Object... v) {
      return "O:" + Arrays.toString(v);
    }

    public String evaluate(long v) {
      return "l:" + v;
    }

    public String evaluate(Long... v) {
      return "L:" + Arrays.toString(v);
    }

    public String evaluate(long... v) {
      return "l:" + Arrays.toString(v);
    }

    public String evaluate(Double... v) {
      return "D:" + Arrays.toString(v);
    }

    public String evaluate(double... v) {
      return "d:" + Arrays.toString(v);
    }

    public String evaluate(float... v) {
      return "f:" + Arrays.toString(v);
    }

    public String evaluate(String... v) {
      return "S:" + Arrays.toString(v);
    }
  }

  public static class TestFunc21 {

    public String evaluate(String... vs) {
      return Arrays.toString(vs);
    }

    public String evaluate(Integer v, String... vs) {
      return "S:" + v + "-" + Arrays.toString(vs);
    }

    public String evaluate(Integer v, Object... vs) {
      return "O:" + v + "-" + Arrays.toString(vs);
    }
  }

  public static class TestFunc22 {

    private LinkedList<String> stack = Lists.newLinkedList();

    public List<String> evaluate(String... vs) {
      for (String v : vs) {
        stack.add(v);
      }
      return Lists.newArrayList(stack);
    }

    public List<String> exclude(String... vs) {
      for (String v : vs) {
        stack.remove(v);
      }
      return Lists.newArrayList(stack);
    }

    public void clear() {
    }
  }

  public static class TestFunc23 {

    public Object evaluate(Integer v) {
      return "I:" + v;
    }

    public Object evaluate(Long v) {
      return "L:" + v;
    }

    public Object evaluate(Double v) {
      return "D:" + v;
    }

    public Object evaluate(Object v) {
      return "O:" + v;
    }

    public Object exclude(Integer v) {
      return "I:" + v;
    }

    public Object exclude(Long v) {
      return "L:" + v;
    }

    public Object exclude(Double v) {
      return "D:" + v;
    }

    public Object exclude(Object v) {
      return "O:" + v;
    }

    public void clear() {
    }
  }

  public static class TestFunc24 {

    public Object evaluate(Integer v) {
      return "I:" + v;
    }

    public Object exclude(Integer v) {
      return "I:" + v;
    }

    public Object evaluate(Long... vs) {
      return "L:" + Arrays.toString(vs);
    }

    public Object exclude(Long... vs) {
      return "L:" + Arrays.toString(vs);
    }

    public void clear() {
    }
  }

  public static class TestFunc25 {

    public Object evaluate(List<String> v) {
      return v;
    }

    public Object evaluate(Map<String, String> v) {
      return v;
    }
  }

  public static class TestFunc26 {

    public Object evaluate(String... v) {
      return Arrays.toString(v);
    }
  }

  @Test
  public void testInvoke() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build();

    assertThat((String) FunctionInvoker.create(TestFunc.class, field("f2")).evaluate(tuple),
        is("I:123"));
    assertThat((String) FunctionInvoker.create(TestFunc2.class, field("f2")).evaluate(tuple),
        is("i:123"));
    assertThat((String) FunctionInvoker.create(TestFunc3.class, field("f2")).evaluate(tuple),
        is("O:123"));
  }

  @Test(expected = FunctionValidateException.class)
  public void testNoMethod() throws Exception {
    FunctionInvoker.validate(TestFunc4.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testInvalidParams() throws Exception {
    FunctionInvoker.validate(TestFunc5.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testInvalidReturnType() throws Exception {
    FunctionInvoker.validate(TestFunc6.class);
  }

  @Test
  public void testValidate() throws Exception {
    FunctionInvoker.validate(TestFunc7.class);
  }

  @Test
  public void testInvokeMultipleArgs() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build();

    assertThat(
        (String) FunctionInvoker.create(TestFunc8.class, field("f1"), field("f2")).evaluate(tuple),
        is("SI:test-123"));
  }

  @Test
  public void testInvokeUDAF() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build();

    FunctionInvoker invoker = FunctionInvoker.create(TestFunc9.class, field("f2"));
    assertThat((Long) invoker.evaluate(tuple), is(123L));
    assertThat((Long) invoker.evaluate(tuple), is(246L));
    assertThat((Long) invoker.evaluate(tuple), is(369L));
    assertThat((Long) invoker.exclude(tuple), is(246L));
    invoker.clear();
    assertThat((Long) invoker.evaluate(tuple), is(123L));
  }

  @Test(expected = FunctionValidateException.class)
  public void testNoMethodUDAF() throws Exception {
    FunctionInvoker.validate(TestFunc10.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testNoMethodUDAF2() throws Exception {
    FunctionInvoker.validate(TestFunc11.class);
  }

  @Test
  public void testNoParamUDAF() throws Exception {
    FunctionInvoker.validate(TestFunc12.class);
  }

  @Test
  public void testValidateParamsUDAF() throws Exception {
    FunctionInvoker.validate(TestFunc13.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testInvalidParamsUDAF2() throws Exception {
    FunctionInvoker.validate(TestFunc14.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testInvalidParamsUDAF3() throws Exception {
    FunctionInvoker.validate(TestFunc15.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testInvalidReturnTypeUDAF() throws Exception {
    FunctionInvoker.validate(TestFunc16.class);
  }

  @Test(expected = FunctionValidateException.class)
  public void testInvalidReturnTypeUDAF2() throws Exception {
    FunctionInvoker.validate(TestFunc17.class);
  }

  @Test
  public void testInvalidUDAF() throws Exception {
    FunctionInvoker.validate(TestFunc18.class);
  }

  @Test
  public void testInvokeNoArgUDAF() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123)
        .put("f3", 456).build();

    FunctionInvoker invoker = FunctionInvoker.create(TestFunc19.class);
    assertThat((Long) invoker.evaluate(tuple), is(1L));
    assertThat((Long) invoker.evaluate(tuple), is(2L));
    assertThat((Long) invoker.evaluate(tuple), is(3L));
    assertThat((Long) invoker.exclude(tuple), is(2L));
    invoker.clear();
    assertThat((Long) invoker.evaluate(tuple), is(1L));

    invoker = FunctionInvoker.create(TestFunc19.class, field("f2"), field("f2"));
    assertThat((Long) invoker.evaluate(tuple), is(2L));
    assertThat((Long) invoker.exclude(tuple), is(0L));
  }

  @Test
  public void testInvokeVarArgs() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3").field("f4")
        .field("f5").field("f6");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", 10L).put("f2", 10).put("f3", 10.0)
        .put("f4", 10.0F).put("f5", "test").put("f6", null).build();

    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f2")).evaluate(tuple),
        is("I:10"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f1")).evaluate(tuple),
        is("l:10"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f3")).evaluate(tuple),
        is("D:[10.0]"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f4")).evaluate(tuple),
        is("f:[10.0]"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f5")).evaluate(tuple),
        is("S:[test]"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f1"), field("f2"))
        .evaluate(tuple), is("O:[10, 10]"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f5"), field("f6"), null)
        .evaluate(tuple), is("S:[test, null, null]"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, null, field("f6"))
        .evaluate(tuple), is("O:[null, null]"));
    assertThat((String) FunctionInvoker.create(TestFunc20.class, field("f2"), null)
        .evaluate(tuple), is("I:[10, null]"));
  }

  @Test
  public void testInvokeVarArgs2() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3").field("f4");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123)
        .put("f3", "yyy").put("f4", 10L).build();

    assertThat((String) FunctionInvoker.create(TestFunc21.class, field("f1"), field("f3"), "xxx")
        .evaluate(tuple), is("[test, yyy, xxx]"));
    assertThat((String) FunctionInvoker.create(TestFunc21.class, field("f2"), field("f3"), "xxx")
        .evaluate(tuple), is("S:123-[yyy, xxx]"));
    assertThat(
        (String) FunctionInvoker.create(TestFunc21.class, field("f2"), field("f4"), field("f3"),
            "xxx").evaluate(tuple),
        is("O:123-[10, yyy, xxx]"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInvokeVarArgsUDAF() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", "123").build();
    GungnirTuple tuple2 = GungnirTuple.builder(schema).put("f1", "test2").put("f2", "1234").build();

    FunctionInvoker invoker =
        FunctionInvoker.create(TestFunc22.class, field("f1"), field("f2"), "xxx");
    assertThat((List<String>) invoker.evaluate(tuple),
        is((List<String>) Lists.newArrayList("test", "123", "xxx")));
    assertThat((List<String>) invoker.evaluate(tuple2),
        is((List<String>) Lists.newArrayList("test", "123", "xxx", "test2", "1234", "xxx")));
    assertThat((List<String>) invoker.exclude(tuple),
        is((List<String>) Lists.newArrayList("test2", "1234", "xxx")));
  }

  @Test
  public void testInvokeNullArgUDAF() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3").field("f4")
        .field("f5");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", 10L).put("f2", 10).put("f3", 10.0)
        .put("f4", null).put("f5", 10F).build();

    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f1")).evaluate(tuple),
        is("L:10"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f2")).evaluate(tuple),
        is("I:10"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f3")).evaluate(tuple),
        is("D:10.0"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f4")).evaluate(tuple),
        is("O:null"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f5")).evaluate(tuple),
        is("O:10.0"));

    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f1")).exclude(tuple),
        is("L:10"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f2")).exclude(tuple),
        is("I:10"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f3")).exclude(tuple),
        is("D:10.0"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f4")).exclude(tuple),
        is("O:null"));
    assertThat((String) FunctionInvoker.create(TestFunc23.class, field("f5")).exclude(tuple),
        is("O:10.0"));
  }

  @Test
  public void testInvokeNullArgUDAF2() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", 10).put("f2", null).put("f3", 20L)
        .build();

    assertThat((String) FunctionInvoker.create(TestFunc24.class, field("f1")).evaluate(tuple),
        is("I:10"));
    assertThat((String) FunctionInvoker.create(TestFunc24.class, field("f2")).evaluate(tuple),
        is("I:null"));
    assertThat((String) FunctionInvoker.create(TestFunc24.class, field("f3")).evaluate(tuple),
        is("L:[20]"));

    assertThat((String) FunctionInvoker.create(TestFunc24.class, field("f1")).exclude(tuple),
        is("I:10"));
    assertThat((String) FunctionInvoker.create(TestFunc24.class, field("f2")).exclude(tuple),
        is("I:null"));
    assertThat((String) FunctionInvoker.create(TestFunc24.class, field("f3")).exclude(tuple),
        is("L:[20]"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInvokeCollectionArg() throws Exception {
    Map<String, Integer> map = Maps.newHashMap();
    map.put("xxx", 12);
    map.put("yyy", 34);
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", Lists.newArrayList("abc", "def"))
        .put("f2", Lists.newArrayList(123, 456)).put("f3", map).build();

    assertThat((ArrayList<String>) FunctionInvoker.create(TestFunc25.class, field("f1"))
        .evaluate(tuple), is(Lists.newArrayList("abc", "def")));
    assertThat((ArrayList<Integer>) FunctionInvoker.create(TestFunc25.class, field("f2"))
        .evaluate(tuple), is(Lists.newArrayList(123, 456)));
    assertThat((HashMap<String, Integer>) FunctionInvoker.create(TestFunc25.class, field("f3"))
        .evaluate(tuple), is(Maps.newHashMap(map)));
  }

  @Test
  public void testInvokeNullVarArg() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", null).put("f2", null).build();

    assertThat((String) FunctionInvoker.create(TestFunc26.class, field("f1")).evaluate(tuple),
        is("[null]"));
    assertThat((String) FunctionInvoker.create(TestFunc26.class, field("f1"), field("f2"))
        .evaluate(tuple), is("[null, null]"));
  }
}
