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

package org.gennai.gungnir.tuple.store;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.tuple.store.Query.ConditionType;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestInMemoryTupleStore {

  private InMemoryTupleStore tupleStore;

  @Before
  public void setup() throws Exception {
    tupleStore = new InMemoryTupleStore();
    tupleStore.open(GungnirManager.getManager().getConfig(), new GungnirContext());

    for (int i = 0; i < 100; i++) {
      String key = String.format("%05d", i);
      for (int j = 10; j <= 28; j += 2) {
        List<Object> values = Lists.newArrayList();
        values.add(key);
        values.add(j);
        for (int k = 0; k < 3; k++) {
          tupleStore.put(String.format("%05d", i), j, values);
        }
      }
    }
  }

  @Test
  public void testCount() throws Exception {
    assertThat(tupleStore.count(Query.builder().hashKeyValue("00055").build()), is(30));

    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 18).build()), is(18));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 18).build()), is(15));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 19).build()), is(15));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 100).build()), is(0));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 100).build()), is(0));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 5).build()), is(30));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 5).build()), is(30));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 28).build()), is(3));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 28).build()), is(0));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 10).build()), is(30));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 10).build()), is(27));

    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 20).build()), is(18));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 20).build()), is(15));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 19).build()), is(15));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 100).build()), is(30));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 100).build()), is(30));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 5).build()), is(0));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 5).build()), is(0));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 28).build()), is(30));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 28).build()), is(27));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 10).build()), is(3));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 10).build()), is(0));

    assertThat(tupleStore.count(Query.builder().hashKeyValue("00055").offset(25).build()), is(5));
    assertThat(tupleStore.count(Query.builder().hashKeyValue("00055").limit(10).build()), is(10));
    assertThat(tupleStore.count(Query.builder().hashKeyValue("00055").offset(10).limit(5).build()),
        is(5));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055").offset(25).limit(10).build()),
        is(5));
    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00055").offset(30).limit(10).build()),
        is(0));
  }

  @Test
  public void testFind() throws Exception {
    List<List<Object>> results = tupleStore.find(Query.builder().hashKeyValue("00055").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 18).build());
    assertThat((Integer) results.get(0).get(1), is(18));
    assertThat((Integer) results.get(17).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 18).build());
    assertThat((Integer) results.get(0).get(1), is(20));
    assertThat((Integer) results.get(14).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 19).build());
    assertThat((Integer) results.get(0).get(1), is(20));
    assertThat((Integer) results.get(14).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 100).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 100).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055").timeKeyCondition(ConditionType.GE, 5)
            .build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055").timeKeyCondition(ConditionType.GT, 5)
            .build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 28).build());
    assertThat((Integer) results.get(0).get(1), is(28));
    assertThat((Integer) results.get(2).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 28).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GE, 10).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.GT, 10).build());
    assertThat((Integer) results.get(0).get(1), is(12));
    assertThat((Integer) results.get(26).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 20).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(17).get(1), is(20));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 20).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(14).get(1), is(18));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 19).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(14).get(1), is(18));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 100).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 100).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055").timeKeyCondition(ConditionType.LE, 5)
            .build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055").timeKeyCondition(ConditionType.LT, 5)
            .build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 28).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 28).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(26).get(1), is(26));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LE, 10).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(2).get(1), is(10));

    results =
        tupleStore.find(Query.builder().hashKeyValue("00055")
            .timeKeyCondition(ConditionType.LT, 10).build());
    assertThat(results.size(), is(0));

    results = tupleStore.find(Query.builder().hashKeyValue("00055").offset(25).build());
    assertThat((Integer) results.get(0).get(1), is(26));
    assertThat((Integer) results.get(4).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00055").limit(10).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(9).get(1), is(16));

    results = tupleStore.find(Query.builder().hashKeyValue("00055").offset(10).limit(5).build());
    assertThat((Integer) results.get(0).get(1), is(16));
    assertThat((Integer) results.get(4).get(1), is(18));

    results = tupleStore.find(Query.builder().hashKeyValue("00055").offset(25).limit(10).build());
    assertThat((Integer) results.get(0).get(1), is(26));
    assertThat((Integer) results.get(4).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00055").offset(30).limit(10).build());
    assertThat(results.size(), is(0));
  }

  // CHECKSTYLE IGNORE MethodLength FOR NEXT 1 LINES
  @Test
  public void testFindAndRemove() throws Exception {
    List<List<Object>> results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00000").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00001")
            .timeKeyCondition(ConditionType.GE, 18).build());
    assertThat((Integer) results.get(0).get(1), is(18));
    assertThat((Integer) results.get(17).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00002")
            .timeKeyCondition(ConditionType.GT, 18).build());
    assertThat((Integer) results.get(0).get(1), is(20));
    assertThat((Integer) results.get(14).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00003")
            .timeKeyCondition(ConditionType.GT, 19).build());
    assertThat((Integer) results.get(0).get(1), is(20));
    assertThat((Integer) results.get(14).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00004")
            .timeKeyCondition(ConditionType.GE, 100).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00005")
            .timeKeyCondition(ConditionType.GT, 100).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00006")
            .timeKeyCondition(ConditionType.GE, 5).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00007")
            .timeKeyCondition(ConditionType.GT, 5).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00008")
            .timeKeyCondition(ConditionType.GE, 28).build());
    assertThat((Integer) results.get(0).get(1), is(28));
    assertThat((Integer) results.get(2).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00009")
            .timeKeyCondition(ConditionType.GT, 28).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00010")
            .timeKeyCondition(ConditionType.GE, 10).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00011")
            .timeKeyCondition(ConditionType.GT, 10).build());
    assertThat((Integer) results.get(0).get(1), is(12));
    assertThat((Integer) results.get(26).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00012")
            .timeKeyCondition(ConditionType.LE, 20).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(17).get(1), is(20));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00013")
            .timeKeyCondition(ConditionType.LT, 20).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(14).get(1), is(18));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00014")
            .timeKeyCondition(ConditionType.LT, 19).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(14).get(1), is(18));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00015")
            .timeKeyCondition(ConditionType.LE, 100).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00016")
            .timeKeyCondition(ConditionType.LT, 100).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00017")
            .timeKeyCondition(ConditionType.LE, 5).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00018")
            .timeKeyCondition(ConditionType.LT, 5).build());
    assertThat(results.size(), is(0));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00019")
            .timeKeyCondition(ConditionType.LE, 28).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00020")
            .timeKeyCondition(ConditionType.LT, 28).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(26).get(1), is(26));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00021")
            .timeKeyCondition(ConditionType.LE, 10).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(2).get(1), is(10));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00022")
            .timeKeyCondition(ConditionType.LT, 10).build());
    assertThat(results.size(), is(0));

    results = tupleStore.findAndRemove(Query.builder().hashKeyValue("00023").offset(25).build());
    assertThat((Integer) results.get(0).get(1), is(26));
    assertThat((Integer) results.get(4).get(1), is(28));

    results = tupleStore.findAndRemove(Query.builder().hashKeyValue("00024").limit(10).build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(9).get(1), is(16));

    results =
        tupleStore.findAndRemove(Query.builder().hashKeyValue("00025").offset(10).limit(5).build());
    assertThat((Integer) results.get(0).get(1), is(16));
    assertThat((Integer) results.get(4).get(1), is(18));

    results =
        tupleStore
            .findAndRemove(Query.builder().hashKeyValue("00026").offset(25).limit(10).build());
    assertThat((Integer) results.get(0).get(1), is(26));
    assertThat((Integer) results.get(4).get(1), is(28));

    results =
        tupleStore
            .findAndRemove(Query.builder().hashKeyValue("00027").offset(30).limit(10).build());
    assertThat(results.size(), is(0));

    results = tupleStore.find(Query.builder().hashKeyValue("00000").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00001").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(11).get(1), is(16));

    results = tupleStore.find(Query.builder().hashKeyValue("00002").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(14).get(1), is(18));

    results = tupleStore.find(Query.builder().hashKeyValue("00003").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(14).get(1), is(18));

    results = tupleStore.find(Query.builder().hashKeyValue("00004").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00005").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00006").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00007").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00008").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(26).get(1), is(26));

    results = tupleStore.find(Query.builder().hashKeyValue("00009").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00010").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00011").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(2).get(1), is(10));

    results = tupleStore.find(Query.builder().hashKeyValue("00012").build());
    assertThat((Integer) results.get(0).get(1), is(22));
    assertThat((Integer) results.get(11).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00013").build());
    assertThat((Integer) results.get(0).get(1), is(20));
    assertThat((Integer) results.get(14).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00014").build());
    assertThat((Integer) results.get(0).get(1), is(20));
    assertThat((Integer) results.get(14).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00015").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00016").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00017").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00018").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00019").build());
    assertThat(results, is(nullValue()));

    results = tupleStore.find(Query.builder().hashKeyValue("00020").build());
    assertThat((Integer) results.get(0).get(1), is(28));
    assertThat((Integer) results.get(2).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00021").build());
    assertThat((Integer) results.get(0).get(1), is(12));
    assertThat((Integer) results.get(26).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00022").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00023").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(24).get(1), is(26));

    results = tupleStore.find(Query.builder().hashKeyValue("00024").build());
    assertThat((Integer) results.get(0).get(1), is(16));
    assertThat((Integer) results.get(19).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00025").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(24).get(1), is(28));

    results = tupleStore.find(Query.builder().hashKeyValue("00026").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(24).get(1), is(26));

    results = tupleStore.find(Query.builder().hashKeyValue("00027").build());
    assertThat((Integer) results.get(0).get(1), is(10));
    assertThat((Integer) results.get(29).get(1), is(28));

    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00004")
            .timeKeyCondition(ConditionType.LT, 14).build()), is(6));

    assertThat(
        tupleStore.count(Query.builder().hashKeyValue("00025")
            .timeKeyCondition(ConditionType.LE, 16).build()), is(10));

    assertThat(tupleStore.count(), is(2609));
  }
}
