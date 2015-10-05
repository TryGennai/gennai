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

package org.gennai.gungnir.topology.operator.tuplejoin;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.tuplejoin.JoinTupleCollection.DispatchHandler;
import org.gennai.gungnir.topology.processor.InMemoryTtlCacheProcessor;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.gennai.gungnir.utils.GungnirUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

@RunWith(JMockit.class)
public class TestJoinTupleCollection {

  // CHECKSTYLE IGNORE MethodLength FOR NEXT 1 LINES
  @Test
  public void testJoin() throws Exception {
    TupleAccessor t1 = new TupleAccessor("t1");
    TupleAccessor t2 = new TupleAccessor("t2");

    SimpleJoinContext simpleContext1 = new SimpleJoinContext(t1,
        Lists.<FieldAccessor>newArrayList(t1.field("f1"), t1.field("f3")));
    simpleContext1.setJoinKey(new SimpleJoinKey(t1.field("f0")));

    SimpleJoinContext simpleContext2 = new SimpleJoinContext(t2,
        Lists.<FieldAccessor>newArrayList(t2.field("f3"), t2.field("f1")));
    simpleContext2.setJoinKey(new SimpleJoinKey(t2.field("f1")));

    ComplexJoinContext complexContext1 = new ComplexJoinContext();
    complexContext1.addContext(simpleContext1);
    complexContext1.addContext(simpleContext2);

    final List<List<Object>> valuesList = Lists.newArrayList();

    GungnirConfig config = GungnirConfig.readGugnirConfig();
    GungnirContext context = new GungnirContext();

    JoinTupleCollection collection = new JoinTupleCollection(complexContext1,
        new InMemoryTtlCacheProcessor(), 10, null, 8);

    collection.prepare(config, context, null);

    collection.setDispatchHandler(new DispatchHandler() {

      @Override
      public void dispatch(List<Object> values) {
        valuesList.add(values);
      }
    });

    GungnirTuple tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "t1f1").put("f2", "t1f2").put("f3", "t1f3").build();
    collection.put(tuple1);

    tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "t1f1-2").put("f2", "t1f2-2").put("f3", "t1f3-2").build();
    collection.put(tuple1);

    tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "t1f1-3").put("f2", "t1f2-3").put("f3", "t1f3-3").build();
    collection.put(tuple1);

    GungnirTuple tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "t2f0").put("f1", "key1").put("f2", "t2f2").put("f3", "t2f3").build();
    collection.put(tuple2);

    assertThat(valuesList.size(), is(3));
    assertThat(valuesList.get(0), is((List<Object>) Lists.<Object>newArrayList("t1f1", "t1f3",
        "t2f3", "key1")));
    assertThat(valuesList.get(1), is((List<Object>) Lists.<Object>newArrayList("t1f1-2", "t1f3-2",
        "t2f3", "key1")));
    assertThat(valuesList.get(2), is((List<Object>) Lists.<Object>newArrayList("t1f1-3", "t1f3-3",
        "t2f3", "key1")));

    collection.cleanup();

    valuesList.clear();

    SimpleJoinContext simpleContext3 = new SimpleJoinContext(t1,
        Lists.<FieldAccessor>newArrayList(t1.field("f1"), t1.field("f3")));
    ComplexJoinKey joinKey = new ComplexJoinKey();
    joinKey.add(new SimpleJoinKey(t1.field("f0")));
    joinKey.add(new SimpleJoinKey(t1.field("f1")));
    simpleContext3.setJoinKey(joinKey);

    SimpleJoinContext simpleContext4 = new SimpleJoinContext(t2,
        Lists.<FieldAccessor>newArrayList(t2.field("f1"), t2.field("f3")));
    joinKey = new ComplexJoinKey();
    joinKey.add(new SimpleJoinKey(t2.field("f0")));
    joinKey.add(new SimpleJoinKey(t2.field("f1")));
    simpleContext4.setJoinKey(joinKey);

    ComplexJoinContext complexContext2 = new ComplexJoinContext();
    complexContext2.addContext(simpleContext3);
    complexContext2.addContext(simpleContext4);

    JoinTupleCollection collection2 = new JoinTupleCollection(complexContext2,
        new InMemoryTtlCacheProcessor(), 10, null, 8);

    collection2.prepare(config, context, null);

    collection2.setDispatchHandler(new DispatchHandler() {

      @Override
      public void dispatch(List<Object> values) {
        valuesList.add(values);
      }
    });

    tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "key2").put("f2", "t2f2").put("f3", "t2f3").build();
    collection2.put(tuple2);

    tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "key2").put("f2", "t2f2-2").put("f3", "t2f3-2").build();
    collection2.put(tuple2);

    tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "key2").put("f2", "t2f2-3").put("f3", "t2f3-3").build();
    collection2.put(tuple2);

    tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key3").put("f1", "key2").put("f2", "t2f2-4").put("f3", "t2f3-4").build();
    collection2.put(tuple2);

    tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key3").put("f1", "key4").put("f2", "t2f2-5").put("f3", "t2f3-5").build();
    collection2.put(tuple2);

    tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "key2").put("f2", "t1f2").put("f3", "t1f3").build();
    collection2.put(tuple1);

    tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key3").put("f1", "key4").put("f2", "t1f2-2").put("f3", "t1f3-2").build();
    collection2.put(tuple1);

    assertThat(valuesList.size(), is(4));
    assertThat(valuesList.get(0), is((List<Object>) Lists.<Object>newArrayList("key2", "t1f3",
        "key2", "t2f3")));
    assertThat(valuesList.get(1), is((List<Object>) Lists.<Object>newArrayList("key2", "t1f3",
        "key2", "t2f3-2")));
    assertThat(valuesList.get(2), is((List<Object>) Lists.<Object>newArrayList("key2", "t1f3",
        "key2", "t2f3-3")));
    assertThat(valuesList.get(3), is((List<Object>) Lists.<Object>newArrayList("key4", "t1f3-2",
        "key4", "t2f3-5")));

    collection2.cleanup();
  }

  // CHECKSTYLE IGNORE MethodLength FOR NEXT 1 LINES
  @Test
  public void testJoin2() throws Exception {
    TupleAccessor t1 = new TupleAccessor("t1");
    TupleAccessor t2 = new TupleAccessor("t2");
    TupleAccessor t3 = new TupleAccessor("t3");
    TupleAccessor t4 = new TupleAccessor("t4");

    SimpleJoinContext simpleContext1 = new SimpleJoinContext(t1,
        Lists.<FieldAccessor>newArrayList(t1.field("f1"), t1.field("f3")));
    simpleContext1.setJoinKey(new SimpleJoinKey(t1.field("f0")));

    SimpleJoinContext simpleContext2 = new SimpleJoinContext(t2,
        Lists.<FieldAccessor>newArrayList(t2.field("f3"), t2.field("f1")));
    simpleContext2.setJoinKey(new SimpleJoinKey(t2.field("f1")));

    SimpleJoinContext simpleContext3 = new SimpleJoinContext(t3,
        Lists.<FieldAccessor>newArrayList(t3.field("f2"), t3.field("f1")));
    simpleContext3.setJoinKey(new SimpleJoinKey(t3.field("f2")));

    ComplexJoinContext complexContext1 = new ComplexJoinContext();
    complexContext1.addContext(simpleContext1);
    complexContext1.addContext(simpleContext2);
    complexContext1.addContext(simpleContext3);
    complexContext1.setJoinKey(new SimpleJoinKey(t3.field("f2")));

    SimpleJoinContext simpleContext4 = new SimpleJoinContext(t4,
        Lists.<FieldAccessor>newArrayList(t4.field("f2"), t4.field("f1")));
    simpleContext4.setJoinKey(new SimpleJoinKey(t4.field("f0")));

    ComplexJoinContext complexContext2 = new ComplexJoinContext();
    complexContext2.addContext(complexContext1);
    complexContext2.addContext(simpleContext4);

    final List<List<Object>> valuesList = Lists.newArrayList();

    GungnirConfig config = GungnirConfig.readGugnirConfig();
    GungnirContext context = new GungnirContext();

    JoinTupleCollection collection = new JoinTupleCollection(complexContext2,
        new InMemoryTtlCacheProcessor(), 10,
        Lists.<FieldAccessor>newArrayList(t4.field("f1"), t4.field("f2"), t3.field("f1"),
            t1.field("f3"), t1.field("f1"), t2.field("f3").as("f4")), 8);

    collection.prepare(config, context, null);

    collection.setDispatchHandler(new DispatchHandler() {

      @Override
      public void dispatch(List<Object> values) {
        valuesList.add(values);
      }
    });

    GungnirTuple tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
            .field("+t3:f2").field("+t3:f1"))
        .put("+t1:f1", "t1f1").put("+t1:f3", "t1f3").put("+t2:f3", "t2f3").put("+t2:f1", "t2f1")
        .put("+t3:f2", "key1").put("+t3:f1", "t3f1").build();
    collection.put(tuple1);

    GungnirTuple tuple4 = GungnirTuple.builder(
        new TupleSchema("t4").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "t4f1").put("f2", "t4f2").put("f3", "t4f3").build();
    collection.put(tuple4);

    tuple4 = GungnirTuple.builder(
        new TupleSchema("t4").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "t4f1-2").put("f2", "t4f2-2").put("f3", "t4f3-2").build();
    collection.put(tuple4);

    tuple4 = GungnirTuple.builder(
        new TupleSchema("t4").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key1").put("f1", "t4f1-3").put("f2", "t4f2-3").put("f3", "t4f3-3").build();
    collection.put(tuple4);

    tuple1 =
        GungnirTuple.builder(
            new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
                .field("+t3:f2").field("+t3:f1"))
            .put("+t1:f1", "t1f1-2").put("+t1:f3", "t1f3-2").put("+t2:f3", "t2f3-2")
            .put("+t2:f1", "t2f1-2").put("+t3:f2", "key1").put("+t3:f1", "t3f1-2").build();
    collection.put(tuple1);

    tuple1 =
        GungnirTuple.builder(
            new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
                .field("+t3:f2").field("+t3:f1"))
            .put("+t1:f1", "t1f1-3").put("+t1:f3", "t1f3-3").put("+t2:f3", "t2f3-3")
            .put("+t2:f1", "t2f1-3").put("+t3:f2", "key1").put("+t3:f1", "t3f1-3").build();
    collection.put(tuple1);

    tuple1 =
        GungnirTuple.builder(
            new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
                .field("+t3:f2").field("+t3:f1"))
            .put("+t1:f1", "t1f1-4").put("+t1:f3", "t1f3-4").put("+t2:f3", "t2f3-4")
            .put("+t2:f1", "t2f1-4").put("+t3:f2", "key2").put("+t3:f1", "t3f1-4").build();
    collection.put(tuple1);

    tuple1 =
        GungnirTuple.builder(
            new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
                .field("+t3:f2").field("+t3:f1"))
            .put("+t1:f1", "t1f1-5").put("+t1:f3", "t1f3-5").put("+t2:f3", "t2f3-5")
            .put("+t2:f1", "t2f1-5").put("+t3:f2", "key2").put("+t3:f1", "t3f1-5").build();
    collection.put(tuple1);

    tuple4 = GungnirTuple.builder(
        new TupleSchema("t4").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key2").put("f1", "t4f1-4").put("f2", "t4f2-4").put("f3", "t4f3-4").build();
    collection.put(tuple4);

    tuple1 =
        GungnirTuple.builder(
            new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
                .field("+t3:f2").field("+t3:f1"))
            .put("+t1:f1", "t1f1-6").put("+t1:f3", "t1f3-6").put("+t2:f3", "t2f3-6")
            .put("+t2:f1", "t2f1-6").put("+t3:f2", "key3").put("+t3:f1", "t3f1-6").build();
    collection.put(tuple1);

    final int now = GungnirUtils.currentTimeSecs();
    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 20;
      }
    };

    tuple1 =
        GungnirTuple.builder(
            new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
                .field("+t3:f2").field("+t3:f1"))
            .put("+t1:f1", "t1f1-7").put("+t1:f3", "t1f3-7").put("+t2:f3", "t2f3-7")
            .put("+t2:f1", "t2f1-7").put("+t3:f2", "key3").put("+t3:f1", "t3f1-7").build();
    collection.put(tuple1);

    tuple4 = GungnirTuple.builder(
        new TupleSchema("t4").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "key3").put("f1", "t4f1-5").put("f2", "t4f2-5").put("f3", "t4f3-5").build();
    collection.put(tuple4);

    assertThat(valuesList.size(), is(6));
    assertThat(valuesList.get(0), is((List<Object>) Lists.<Object>newArrayList("t4f1", "t4f2",
        "t3f1", "t1f3", "t1f1", "t2f3")));

    assertThat(valuesList.get(1), is((List<Object>) Lists.<Object>newArrayList("t4f1-2", "t4f2-2",
        "t3f1-2", "t1f3-2", "t1f1-2", "t2f3-2")));

    assertThat(valuesList.get(2), is((List<Object>) Lists.<Object>newArrayList("t4f1-3", "t4f2-3",
        "t3f1-2", "t1f3-2", "t1f1-2", "t2f3-2")));

    assertThat(valuesList.get(3), is((List<Object>) Lists.<Object>newArrayList("t4f1-4", "t4f2-4",
        "t3f1-4", "t1f3-4", "t1f1-4", "t2f3-4")));

    assertThat(valuesList.get(4), is((List<Object>) Lists.<Object>newArrayList("t4f1-4", "t4f2-4",
        "t3f1-5", "t1f3-5", "t1f1-5", "t2f3-5")));

    assertThat(valuesList.get(5), is((List<Object>) Lists.<Object>newArrayList("t4f1-5", "t4f2-5",
        "t3f1-7", "t1f3-7", "t1f1-7", "t2f3-7")));

    collection.cleanup();
  }
}
