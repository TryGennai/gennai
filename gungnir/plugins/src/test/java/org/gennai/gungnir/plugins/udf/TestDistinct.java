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

package org.gennai.gungnir.plugins.udf;

import static org.gennai.gungnir.ql.QueryOperations.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.gennai.gungnir.FunctionInvoker;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Test;

public class TestDistinct {

  @Test
  public void testInvoke() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build();
    GungnirTuple tuple2 = GungnirTuple.builder(schema).put("f1", "test").put("f2", 37).build();
    GungnirTuple tuple3 = GungnirTuple.builder(schema).put("f1", "test").put("f2", 20).build();
    GungnirTuple tuple4 = GungnirTuple.builder(schema).put("f1", "test").put("f2", 100.5).build();

    FunctionInvoker invoker = FunctionInvoker.create(Distinct.class, field("f2"));
    assertThat((Integer) invoker.evaluate(tuple), is(123));
    assertThat((Integer) invoker.evaluate(tuple2), is(37));
    assertThat((Integer) invoker.evaluate(tuple3), is(20));
    assertThat((Integer) invoker.evaluate(tuple2), nullValue());
    assertThat((Integer) invoker.evaluate(tuple), nullValue());
    invoker.clear();
    assertThat((Integer) invoker.evaluate(tuple3), is(20));
    assertThat((Double) invoker.evaluate(tuple4), is(100.5));
    assertThat((Double) invoker.evaluate(tuple4), nullValue());
  }
}
