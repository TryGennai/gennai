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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class InMemoryTupleStore implements TupleStore {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private static class TupleEntities {

    private List<List<Object>> tuples = Lists.newArrayList();
    private List<Integer> timeKeyIndex = Lists.newArrayList();
  }

  private transient Map<Object, TupleEntities> entitiesMap;
  private transient int size;
  private transient boolean open = false;

  @Override
  public void open(GungnirConfig config, GungnirContext context) {
    entitiesMap = Maps.newHashMap();
    open = true;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void put(Object hashKeyValue, int timeKeyValue, List<Object> values) {
    TupleEntities entities = entitiesMap.get(hashKeyValue);
    if (entities == null) {
      entities = new TupleEntities();
      entitiesMap.put(hashKeyValue, entities);
    }
    entities.tuples.add(Lists.newArrayList(values));
    entities.timeKeyIndex.add(timeKeyValue);
    size++;
  }

  @Override
  public int count() {
    return size;
  }

  private static int search(List<Integer> timeKeyIndex, int value) {
    int index = Collections.binarySearch(timeKeyIndex, value);
    if (index < 0) {
      index *= -1;
      index--;
    }
    return index;
  }

  private static class Position {

    private int from;
    private int to;

    Position(int from, int to) {
      this.from = from;
      this.to = to;
    }
  }

  private Position searchTimeKey(List<Integer> timeKeyIndex, Query query) {
    switch (query.getTimeKeyConditionType()) {
      case GT:
        if (timeKeyIndex.get(0) > query.getTimeKeyValue()) {
          return new Position(0, timeKeyIndex.size());
        } else if (timeKeyIndex.get(timeKeyIndex.size() - 1) < query.getTimeKeyValue()) {
          return new Position(timeKeyIndex.size(), timeKeyIndex.size());
        } else {
          int index = search(timeKeyIndex, query.getTimeKeyValue());
          for (int i = index; i < timeKeyIndex.size(); i++) {
            if (timeKeyIndex.get(i) > query.getTimeKeyValue()) {
              return new Position(i, timeKeyIndex.size());
            }
          }
          return new Position(timeKeyIndex.size(), timeKeyIndex.size());
        }
      case GE:
        if (timeKeyIndex.get(0) > query.getTimeKeyValue()) {
          return new Position(0, timeKeyIndex.size());
        } else if (timeKeyIndex.get(timeKeyIndex.size() - 1) < query.getTimeKeyValue()) {
          return new Position(timeKeyIndex.size(), timeKeyIndex.size());
        } else {
          int index = search(timeKeyIndex, query.getTimeKeyValue());
          for (int i = index; i >= 0; i--) {
            if (timeKeyIndex.get(i) < query.getTimeKeyValue()) {
              return new Position(i + 1, timeKeyIndex.size());
            }
          }
          return new Position(0, timeKeyIndex.size());
        }
      case LT:
        if (timeKeyIndex.get(0) > query.getTimeKeyValue()) {
          return new Position(0, 0);
        } else if (timeKeyIndex.get(timeKeyIndex.size() - 1) < query.getTimeKeyValue()) {
          return new Position(0, timeKeyIndex.size());
        } else {
          int index = search(timeKeyIndex, query.getTimeKeyValue());
          for (int i = index; i >= 0; i--) {
            if (timeKeyIndex.get(i) < query.getTimeKeyValue()) {
              return new Position(0, i + 1);
            }
          }
          return new Position(0, 0);
        }
      case LE:
        if (timeKeyIndex.get(0) > query.getTimeKeyValue()) {
          return new Position(0, 0);
        } else if (timeKeyIndex.get(timeKeyIndex.size() - 1) < query.getTimeKeyValue()) {
          return new Position(0, timeKeyIndex.size());
        } else {
          int index = search(timeKeyIndex, query.getTimeKeyValue());
          for (int i = index; i < timeKeyIndex.size(); i++) {
            if (timeKeyIndex.get(i) > query.getTimeKeyValue()) {
              return new Position(0, i);
            }
          }
          return new Position(0, timeKeyIndex.size());
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  private Position getPosition(TupleEntities entities, Query query) {
    if (query.getTimeKeyConditionType() != null && query.getTimeKeyValue() != null) {
      return searchTimeKey(entities.timeKeyIndex, query);
    } else if (query.getOffset() != null || query.getLimit() != null) {
      int from;
      if (query.getOffset() != null && query.getOffset() >= 0) {
        if (query.getOffset() < entities.tuples.size()) {
          from = query.getOffset();
        } else {
          from = entities.tuples.size();
        }
      } else {
        from = 0;
      }
      int to = 0;
      if (query.getLimit() != null && query.getLimit() + from < entities.tuples.size()) {
        to = query.getLimit() + from;
      } else {
        to = entities.tuples.size();
      }
      return new Position(from, to);
    }
    return new Position(0, entities.tuples.size());
  }

  @Override
  public int count(Query query) {
    TupleEntities entities = entitiesMap.get(query.getHashKeyValue());
    if (entities != null) {
      Position position = getPosition(entities, query);
      return position.to - position.from;
    } else {
      return 0;
    }
  }

  @Override
  public List<List<Object>> find(Query query) {
    TupleEntities entities = entitiesMap.get(query.getHashKeyValue());
    if (entities != null) {
      Position position = getPosition(entities, query);
      return Lists.newArrayList(entities.tuples.subList(position.from, position.to));
    } else {
      return null;
    }
  }

  private int remove(TupleEntities entities, Position position) {
    if (position.from == 0) {
      if (entities.tuples.size() <= position.to) {
        return 0;
      }

      entities.tuples =
          Lists.newArrayList(entities.tuples.subList(position.to, entities.tuples.size()));
      entities.timeKeyIndex =
          Lists.newArrayList(entities.timeKeyIndex.subList(position.to,
              entities.timeKeyIndex.size()));
    } else if (position.to == entities.tuples.size()) {
      if (position.from <= 0) {
        return 0;
      }

      entities.tuples = Lists.newArrayList(entities.tuples.subList(0, position.from));
      entities.timeKeyIndex = Lists.newArrayList(entities.timeKeyIndex.subList(0, position.from));
    } else {
      List<List<Object>> tuples = Lists.newArrayList(entities.tuples.subList(0, position.from));
      tuples.addAll(entities.tuples.subList(position.to, entities.tuples.size()));
      entities.tuples = tuples;
      List<Integer> timeKeyIndex =
          Lists.newArrayList(entities.timeKeyIndex.subList(0, position.from));
      timeKeyIndex.addAll(entities.timeKeyIndex.subList(position.to, entities.tuples.size()));
      entities.timeKeyIndex = timeKeyIndex;
    }

    return entities.tuples.size();
  }

  @Override
  public List<List<Object>> findAndRemove(Query query) {
    TupleEntities entities = entitiesMap.get(query.getHashKeyValue());
    if (entities != null) {
      Position position = getPosition(entities, query);
      List<List<Object>> results =
          Lists.newArrayList(entities.tuples.subList(position.from, position.to));

      size -= entities.tuples.size();
      int sz = remove(entities, position);
      if (sz > 0) {
        size += sz;
      } else {
        entitiesMap.remove(query.getHashKeyValue());
      }

      return results;
    } else {
      return null;
    }
  }

  @Override
  public void remove(Query query) {
    TupleEntities entities = entitiesMap.get(query.getHashKeyValue());
    if (entities != null) {
      Position position = getPosition(entities, query);

      size -= entities.tuples.size();
      int sz = remove(entities, position);
      if (sz > 0) {
        size += sz;
      } else {
        entitiesMap.remove(query.getHashKeyValue());
      }
    }
  }

  @Override
  public void close() {
    open = false;
  }
}
