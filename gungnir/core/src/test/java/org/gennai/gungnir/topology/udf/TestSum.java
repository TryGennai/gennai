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

package org.gennai.gungnir.topology.udf;

import static org.junit.Assert.*;

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Before;
import org.junit.Test;

public class TestSum {

  private TupleSchema schema;

  @Before
  public void setup() {
    schema = new TupleSchema("dummy").field("height").field("width");
  }

  @Test
  public void testEvaluate() throws Exception {
    GungnirTuple tuple = GungnirTuple.builder(schema)
        .put("height", new Integer(100))
        .put("width", new Integer(200))
        .build();
    Sum sum = new Sum().create(new FieldAccessor("height"));
    sum.prepare();
    assertEquals(new Long(100), sum.evaluate(tuple));
  }

  @Test
  public void testMultipleEvaluate() throws Exception {
    GungnirTuple tuple = GungnirTuple.builder(schema)
        .put("height", new Integer(100))
        .put("width", new Integer(200))
        .build();
    Sum sum = new Sum().create(new FieldAccessor("height"));
    sum.prepare();
    assertEquals(new Long(100), sum.evaluate(tuple));
    assertEquals(new Long(200), sum.evaluate(tuple));
    assertEquals(new Long(300), sum.evaluate(tuple));
  }

  @Test
  public void testExclude() throws Exception {
    GungnirTuple tuple = GungnirTuple.builder(schema)
        .put("height", new Integer(100))
        .put("width", new Integer(200))
        .build();
    Sum sum = new Sum().create(new FieldAccessor("height"));
    sum.prepare();
    assertEquals(new Long(100), sum.evaluate(tuple));
    assertEquals(new Long(0), sum.exclude(tuple));
  }

  @Test
  public void testClear() throws Exception {
    GungnirTuple tuple = GungnirTuple.builder(schema)
        .put("height", new Integer(100))
        .put("width", new Integer(200))
        .build();
    Sum sum = new Sum().create(new FieldAccessor("height"));
    sum.prepare();
    assertEquals(new Long(100), sum.evaluate(tuple));
    sum.clear();
    assertEquals(new Long(100), sum.evaluate(tuple));
  }
}
