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

package org.gennai.gungnir.topology;

import static org.gennai.gungnir.ql.QueryOperations.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestConditionEvaluator {

  @Test
  public void testCompare() {
    List<Object> values = Lists.newArrayList();
    values.add("test");
    values.add((byte) 100);
    values.add((short) 100);
    values.add((int) 100);
    values.add((long) 100);
    values.add((float) 100);
    values.add((double) 100);
    values.add(true);
    values.add(null);
    Struct struct1 = new Struct(Lists.newArrayList("bigint"), Lists.<Object>newArrayList(123L));
    values.add(struct1);
    List<Float> list1 = Lists.newArrayList();
    list1.add(1.5f);
    list1.add(3.0f);
    list1.add(5.5f);
    values.add(list1);
    Map<Double, String> map1 = Maps.newLinkedHashMap();
    map1.put(2.5, "A");
    map1.put(6.5, "B");
    values.add(map1);

    TupleValues tupleValues = new TupleValues("tuple1", values);
    GungnirTuple tuple =
        new GungnirTuple(Lists.newArrayList("string", "tinyint", "smallint", "int", "bigint",
            "float", "double", "boolean", "null", "struct1", "list1", "map1"), tupleValues);

    assertTrue(ConditionEvaluator.isKeep(field("tinyint").eq(100), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("smallint").eq(100L), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("bigint").eq(100), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("int").ge(3), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("int").le(100L), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("int").ge(100L), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("int").lt(100), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("int").gt(100), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("int").ge("3"), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("double").between(0, 200), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("float").eq(100.0), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("boolean").eq(true), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("null").isNull(), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("null").isNotNull(), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("string").in("test", "test2"), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("string").like("te%"), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("string").like("t__t"), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("string").regexp("^t[a-z]{3}$"), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("string").all("test"), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("struct1").eq(1), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("struct1").field("bigint").eq(123), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("list1").select(0).eq(1.5), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("list1").all(1.5, 3.0), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("list1").all(1.5, 4.0), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("list1").in(1.5), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("list1").select(1).ne(3.0), tuple));
    assertTrue(ConditionEvaluator.isKeep(field("map1").select(2.5).eq("A"), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("map1").le(3), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("struct1").field("bigint").eq("123"), tuple));
    assertFalse(ConditionEvaluator.isKeep(field("struct1").field("bigint").like("1%"), tuple));
    assertTrue(ConditionEvaluator.isKeep(and(field("tinyint").ge(10), field("smallint").le(100)),
        tuple));
    assertFalse(ConditionEvaluator.isKeep(and(field("tinyint").lt(10), field("smallint").gt(100)),
        tuple));
    assertTrue(ConditionEvaluator.isKeep(
        or(field("list1").select(0).ne(3.0), field("smallint").gt(100)), tuple));
    assertFalse(ConditionEvaluator.isKeep(not(field("int").ge(3)), tuple));
    assertFalse(ConditionEvaluator.isKeep(
        and(field("tinyint").ge(10), not(field("smallint").le(100))), tuple));
  }
}
