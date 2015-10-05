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

import org.gennai.gungnir.ScriptFunctionInvoker;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Test;

public class TestConcatPy {

  @Test
  public void testInvoke() throws Exception {
    TupleSchema schema = new TupleSchema("tuple1").field("f1").field("f2");
    GungnirTuple tuple = GungnirTuple.builder(schema).put("f1", "test").put("f2", 123).build();

    assertThat((String) ScriptFunctionInvoker.create("concat.py", field("f1"), "-", field("f2"))
        .evaluate(tuple), is("test-123"));
  }
}
