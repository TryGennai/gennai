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

import java.util.LinkedList;

import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestCount {

  private Count count;

  @Before
  public void setup() throws Exception {
    count = new Count().create();
    count.prepare();
  }

  @Test
  public void testEvaluateOnce() throws Exception {
    GungnirTuple tuple = new GungnirTuple(
        new LinkedList<String>(),
        new TupleValues("dummy", Lists.newArrayList()));
    assertEquals(new Long(1L), count.evaluate(tuple));
  }

  @Test
  public void testMultipleCallEvaluate() throws Exception {
    GungnirTuple tuple = new GungnirTuple(
        new LinkedList<String>(),
        new TupleValues("dummy", Lists.newArrayList()));
    assertEquals(new Long(1L), count.evaluate(tuple));
    assertEquals(new Long(2L), count.evaluate(tuple));
    assertEquals(new Long(3L), count.evaluate(tuple));
  }

  @Test
  public void testExclude() throws Exception {
    GungnirTuple tuple = new GungnirTuple(
        new LinkedList<String>(),
        new TupleValues("dummy", Lists.newArrayList()));
    assertEquals(new Long(1L), count.evaluate(tuple));
    assertEquals(new Long(0L), count.exclude(tuple));
    assertEquals(new Long(-1L), count.exclude(tuple));
  }

  @Test
  public void testClear() throws Exception {
    GungnirTuple tuple = new GungnirTuple(
        new LinkedList<String>(),
        new TupleValues("dummy", Lists.newArrayList()));
    assertEquals(new Long(1L), count.evaluate(tuple));
    count.clear();
    assertEquals(new Long(-1L), count.exclude(tuple));
  }
}
