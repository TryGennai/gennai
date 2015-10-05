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

package org.gennai.gungnir.topology.operator.slide;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MemoryWindowStorage implements WindowStorage {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private static class Container {

    private List<List<Object>> tuples = Lists.newArrayList();
    private List<Integer> timeKeyIndex = Lists.newArrayList();
  }

  private Map<Object, Container> tuplesMap;

  @Override
  public void open(String topologyId, String operatorName) {
    tuplesMap = Maps.newHashMap();
  }

  @Override
  public void put(Object key, List<Object> values) {
    Container container = tuplesMap.get(key);
    if (container == null) {
      container = new Container();
      tuplesMap.put(key, container);
    }
    container.tuples.add(values);
  }

  @Override
  public void put(Object key, Date timeKey, List<Object> values) {
    Container container = tuplesMap.get(key);
    if (container == null) {
      container = new Container();
      tuplesMap.put(key, container);
    }
    container.tuples.add(values);
    container.timeKeyIndex.add((int) TimeUnit.MILLISECONDS.toSeconds(timeKey.getTime()));
  }

  @Override
  public List<List<Object>> get(Object key, int count) {
    Container container = tuplesMap.get(key);
    if (container == null) {
      return null;
    }

    if (container.tuples.size() <= count) {
      return null;
    }
    List<List<Object>> tuples = Lists.newArrayList(container.tuples.subList(0,
        container.tuples.size() - count));
    container.tuples = Lists.newArrayList(container.tuples.subList(container.tuples.size() - count,
        container.tuples.size()));
    return tuples;
  }

  @Override
  public List<List<Object>> get(Object key, Date timeKey) {
    Container container = tuplesMap.get(key);
    if (container == null) {
      return null;
    }
    int index = Collections.binarySearch(container.timeKeyIndex,
        (int) TimeUnit.MILLISECONDS.toSeconds(timeKey.getTime()));
    if (index < 0) {
      index *= -1;
      index--;
    }

    if (index == 0) {
      return null;
    }
    if (index >= container.tuples.size()) {
      List<List<Object>> tuples = container.tuples;
      container.tuples = Lists.newArrayList();
      container.timeKeyIndex = Lists.newArrayList();
      return tuples;
    }
    List<List<Object>> tuples = Lists.newArrayList(container.tuples.subList(0, index));
    container.tuples = Lists.newArrayList(container.tuples.subList(index, container.tuples.size()));
    container.timeKeyIndex = Lists.newArrayList(container.timeKeyIndex.subList(index,
        container.timeKeyIndex.size()));
    return tuples;
  }

  @Override
  public void close() {
  }
}
