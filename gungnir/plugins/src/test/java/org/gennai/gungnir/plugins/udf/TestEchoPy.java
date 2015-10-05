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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.guava.collect.Lists;
import org.gennai.gungnir.ScriptFunctionInvoker;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestEchoPy {

  @Test
  @SuppressWarnings("unchecked")
  public void testInvoke() throws Exception {
    Map<String, Object> map = Maps.newHashMap();
    map.put("xxx", 12);
    map.put("yyy", 34);
    Map<String, Object> map2 = Maps.newHashMap();
    map2.put("xxx", 12);
    map2.put("yyy", 34);
    map2.put("py", "value");
    Struct struct = new Struct(Lists.newArrayList("s1", "s2"),
        Lists.<Object>newArrayList("v1", "v2"));

    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2").field("f3").field("f4")
        .field("f5");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123)
        .put("f3", Lists.newArrayList("abc", "def")).put("f4", map).put("f5", struct).build();

    assertThat((String) ScriptFunctionInvoker.create("echo.py", field("f1"))
        .evaluate(tuple), is("py 1:test"));
    assertThat((String) ScriptFunctionInvoker.create("echo.py", field("f2"))
        .evaluate(tuple), is("py 1:123"));
    assertThat((String) ScriptFunctionInvoker.create("echo.py", "test2")
        .evaluate(tuple), is("py 1:test2"));
    assertThat((ArrayList<String>) ScriptFunctionInvoker.create("echo.py", field("f3"))
        .evaluate(tuple), is(Lists.newArrayList("abc", "def", "py")));
    assertThat((HashMap<String, Object>) ScriptFunctionInvoker.create("echo.py", field("f4"))
        .evaluate(tuple), is(map2));
    assertThat((ArrayList<Object>) ScriptFunctionInvoker.create("echo.py", field("f5"))
        .evaluate(tuple), is(Lists.<Object>newArrayList("v1", "v2")));
  }
}
