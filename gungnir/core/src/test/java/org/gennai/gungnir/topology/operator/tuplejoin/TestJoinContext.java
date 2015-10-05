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

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestJoinContext {

  @Test
  public void testSimpleContext() throws Exception {
    TupleAccessor t1 = new TupleAccessor("t1");
    TupleAccessor t2 = new TupleAccessor("t2");

    SimpleJoinContext simpleContext1 = new SimpleJoinContext(t1,
        Lists.<FieldAccessor>newArrayList(t1.field("f1"), t1.field("f3")));
    simpleContext1.setJoinKey(new SimpleJoinKey(t1.field("f0")));

    FieldAccessor t1f1 = new FieldAccessor("+t1:f1");
    FieldAccessor t1f3 = new FieldAccessor("+t1:f3");
    FieldAccessor t2f1 = new FieldAccessor("+t2:f1");
    FieldAccessor t2f3 = new FieldAccessor("+t2:f3");

    GungnirTuple tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "t1f0").put("f1", "t1f1").put("f2", "t1f2").put("f3", "t1f3").build();
    GungnirTuple tuple2 = GungnirTuple.builder(
        new TupleSchema("t2").field("f0").field("f1").field("f2").field("f3"))
        .put("f0", "t2f0").put("f1", "t2f1").put("f2", "t2f2").put("f3", "t2f3").build();

    assertThat(simpleContext1.getFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1.field("f1"),
            t1.field("f3"))));
    assertThat(simpleContext1.getJoinKey(), is((JoinKey) new SimpleJoinKey(t1.field("f0"))));
    assertThat(simpleContext1.getOutputFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3)));
    assertThat(simpleContext1.getKey(tuple1), is((Object) "t1f0"));
    assertThat(simpleContext1.getValues(tuple1),
        is((List<Object>) Lists.<Object>newArrayList("t1f1", "t1f3")));

    SimpleJoinContext simpleContext2 = new SimpleJoinContext(t2,
        Lists.<FieldAccessor>newArrayList(t2.field("f3"), t2.field("f1")));
    simpleContext2.setJoinKey(new SimpleJoinKey(t2.field("f1")));

    assertThat(simpleContext2.getFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t2.field("f3"),
            t2.field("f1"))));
    assertThat(simpleContext2.getJoinKey(), is((JoinKey) new SimpleJoinKey(t2.field("f1"))));
    assertThat(simpleContext2.getOutputFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t2f3, t2f1)));
    assertThat(simpleContext2.getKey(tuple2), is((Object) "t2f1"));
    assertThat(simpleContext2.getValues(tuple2),
        is((List<Object>) Lists.<Object>newArrayList("t2f3", "t2f1")));

    SimpleJoinContext simpleContext3 = new SimpleJoinContext(t1,
        Lists.<FieldAccessor>newArrayList(t1.field("f1"), t1.field("f3")));
    ComplexJoinKey joinKey = new ComplexJoinKey();
    joinKey.add(new SimpleJoinKey(t1.field("f0")));
    joinKey.add(new SimpleJoinKey(t1.field("f1")));
    simpleContext3.setJoinKey(joinKey);

    assertThat(simpleContext3.getFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1.field("f1"),
            t1.field("f3"))));
    assertThat(simpleContext3.getJoinKey(), is((JoinKey) joinKey));
    assertThat(simpleContext3.getOutputFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3)));
    assertThat(simpleContext3.getKey(tuple1), is((Object) Lists.newArrayList("t1f0", "t1f1")));
    assertThat(simpleContext3.getValues(tuple1),
        is((List<Object>) Lists.<Object>newArrayList("t1f1", "t1f3")));
  }

  @Test
  public void testComplexContext() throws Exception {
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
    complexContext1.setJoinKey(new SimpleJoinKey(t2.field("f2")));

    FieldAccessor t1f1 = new FieldAccessor("+t1:f1");
    FieldAccessor t1f3 = new FieldAccessor("+t1:f3");
    FieldAccessor t2f1 = new FieldAccessor("+t2:f1");
    FieldAccessor t2f2 = new FieldAccessor("+t2:f2");
    FieldAccessor t2f3 = new FieldAccessor("+t2:f3");
    FieldAccessor t3f1 = new FieldAccessor("+t3:f1");
    FieldAccessor t3f2 = new FieldAccessor("+t3:f2");

    GungnirTuple tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t1:f0").field("+t2:f3")
            .field("+t2:f1").field("+t3:f2").field("+t3:f1").field("+t2:f2"))
        .put("+t1:f1", "t1f1").put("+t1:f3", "t1f3").put("+t1:f0", "t1f0").put("+t2:f3", "t2f3")
        .put("+t2:f1", "t2f1").put("+t3:f2", "t3f2").put("+t3:f1", "t3f1").put("+t2:f2", "t2f2")
        .build();

    assertThat(complexContext1.getFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3, t2f3, t2f1, t3f2,
            t3f1, t2f2)));
    assertThat(complexContext1.getJoinKey(), is((JoinKey) new SimpleJoinKey(t2.field("f2"))));
    assertThat(complexContext1.getOutputFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3, t2f3, t2f1, t3f2,
            t3f1, t2f2)));
    assertThat(complexContext1.getKey(tuple1), is((Object) "t2f2"));
    assertThat(complexContext1.getValues(tuple1),
        is((List<Object>) Lists.<Object>newArrayList("t1f1", "t1f3", "t1f0", "t2f3", "t2f1",
            "t3f2", "t3f1", "t2f2")));

    ComplexJoinContext complexContext2 = new ComplexJoinContext();
    complexContext2.addContext(simpleContext1);
    complexContext2.addContext(simpleContext2);
    complexContext2.addContext(simpleContext3);
    complexContext2.setJoinKey(new SimpleJoinKey(t3.field("f2")));

    tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t1:f0").field("+t2:f3")
            .field("+t2:f1").field("+t2:f2").field("+t3:f2").field("+t3:f1"))
        .put("+t1:f1", "t1f1").put("+t1:f3", "t1f3").put("+t1:f0", "t1f0").put("+t2:f3", "t2f3")
        .put("+t2:f1", "t2f1").put("+t2:f2", "t2f2").put("+t3:f2", "t3f2").put("+t3:f1", "t3f1")
        .build();

    assertThat(complexContext2.getFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3, t2f3, t2f1, t2f2,
            t3f2, t3f1)));
    assertThat(complexContext2.getJoinKey(), is((JoinKey) new SimpleJoinKey(t3.field("f2"))));
    assertThat(complexContext2.getOutputFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3, t2f3, t2f1, t2f2,
            t3f2, t3f1)));
    assertThat(complexContext2.getKey(tuple1), is((Object) "t3f2"));
    assertThat(complexContext2.getValues(tuple1),
        is((List<Object>) Lists.<Object>newArrayList("t1f1", "t1f3", "t1f0", "t2f3", "t2f1",
            "t2f2", "t3f2", "t3f1")));

    SimpleJoinContext simpleContext4 = new SimpleJoinContext(t4,
        Lists.<FieldAccessor>newArrayList(t4.field("f2"), t4.field("f1")));
    simpleContext4.setJoinKey(new SimpleJoinKey(t4.field("f0")));

    ComplexJoinContext complexContext3 = new ComplexJoinContext();
    complexContext3.addContext(complexContext2);
    complexContext3.addContext(simpleContext4);
    complexContext3.setJoinKey(new SimpleJoinKey(t2.field("f1")));

    FieldAccessor t4f2 = new FieldAccessor("+t4:f2");
    FieldAccessor t4f1 = new FieldAccessor("+t4:f1");

    tuple1 = GungnirTuple.builder(
        new TupleSchema("t1").field("+t1:f1").field("+t1:f3").field("+t2:f3").field("+t2:f1")
            .field("+t2:f2").field("+t3:f2").field("+t3:f1").field("+t4:f2").field("+t4:f1"))
        .put("+t1:f1", "t1f1").put("+t1:f3", "t1f3").put("+t2:f3", "t2f3").put("+t2:f1", "t2f1")
        .put("+t2:f2", "t2f2").put("+t3:f2", "t3f2").put("+t3:f1", "t3f1").put("+t4:f2", "t4f2")
        .put("+t4:f1", "t4f1").build();

    assertThat(complexContext3.getFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3, t2f3, t2f1, t2f2,
            t3f2, t3f1, t4f2, t4f1)));
    assertThat(complexContext3.getJoinKey(), is((JoinKey) new SimpleJoinKey(t2.field("f1"))));
    assertThat(complexContext3.getOutputFields(),
        is((List<FieldAccessor>) Lists.<FieldAccessor>newArrayList(t1f1, t1f3, t2f3, t2f1, t2f2,
            t3f2, t3f1, t4f2, t4f1)));
    assertThat(complexContext3.getKey(tuple1), is((Object) "t2f1"));
    assertThat(complexContext3.getValues(tuple1),
        is((List<Object>) Lists.<Object>newArrayList("t1f1", "t1f3", "t2f3", "t2f1", "t2f2",
            "t3f2", "t3f1", "t4f2", "t4f1")));
  }
}
