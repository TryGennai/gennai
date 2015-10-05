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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.store.Query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GroupedTupleCollection extends BaseTupleCollection {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public interface OutputHandler {

    void output(List<Object> values);
  }

  private List<TupleCollection> collections;
  private Period expire;
  private OutputHandler outputHandler;
  private Map<String, TupleCollection> collectionsMap;
  private int[] outputFieldsIndex;
  private boolean[] isOutputAllFields;

  public GroupedTupleCollection() {
    super();
    collections = Lists.newArrayList();
  }

  private GroupedTupleCollection(GroupedTupleCollection c) {
    super(c);
    this.collections = c.collections;
    this.expire = c.expire;
    this.collectionsMap = c.collectionsMap;
    this.outputFieldsIndex = c.outputFieldsIndex;
    this.isOutputAllFields = c.isOutputAllFields;
  }

  @Override
  public TupleAccessor getFromTuple() {
    if (collections.size() > 0) {
      return collections.get(0).getFromTuple();
    } else {
      return null;
    }
  }

  public List<TupleCollection> getCollections() {
    return collections;
  }

  public void addCollection(TupleCollection collection) {
    collections.add(collection);
    getFields().addAll(collection.getOutputFields());
  }

  public TupleCollection getCollection(String tupleName) {
    if (collectionsMap == null) {
      collectionsMap = Maps.newHashMap();
      for (TupleCollection collection : collections) {
        collectionsMap.put(collection.getFromTuple().getTupleName(), collection);
      }
    }
    return collectionsMap.get(tupleName);
  }

  @Override
  public void setJoinKey(JoinKey joinKey) {
    super.setJoinKey(joinKey);
    for (TupleCollection collection : collections) {
      if (collection.getFromTuple().getTupleName().equals(joinKey.getTupleName())) {
        if (joinKey instanceof ComplexJoinKey) {
          for (SimpleJoinKey simpleKey : ((ComplexJoinKey) joinKey).getJoinKeys()) {
            if (!collection.getFields().contains(simpleKey.getKeyField())) {
              collection.getFields().add(simpleKey.getKeyField());
              collection.getOutputFields().add(getJoinField(simpleKey.getKeyField()));
            }
          }
        } else {
          if (!collection.getFields().contains(((SimpleJoinKey) joinKey).getKeyField())) {
            collection.getFields().add(((SimpleJoinKey) joinKey).getKeyField());
            collection.getOutputFields().add(getJoinField(((SimpleJoinKey) joinKey).getKeyField()));
          }
        }
      }
    }
  }

  public void setOutputHandler(OutputHandler outputHandler) {
    this.outputHandler = outputHandler;
  }

  @Override
  public List<FieldAccessor> getOutputFields() {
    if (super.getOutputFields() != null) {
      return super.getOutputFields();
    } else {
      List<FieldAccessor> outputFields = Lists.newArrayList();
      for (TupleCollection collection : collections) {
        outputFields.addAll(collection.getOutputFields());
      }
      return outputFields;
    }
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context) {
    super.open(config, context);
    for (TupleCollection collection : getCollections()) {
      collection.open(config, context);
    }
  }

  @SuppressWarnings("unchecked")
  private void output(List<Object> values) {
    if (outputFieldsIndex == null) {
      List<FieldAccessor> outputFields = null;
      if (super.getOutputFields() != null) {
        outputFields = super.getOutputFields();
      } else {
        outputFields = Lists.newArrayList();
      }
      List<FieldAccessor> fields = Lists.newArrayList();
      for (TupleCollection collection : collections) {
        for (FieldAccessor field : collection.getFields()) {
          fields.add(getJoinField(field));
        }
        if (super.getOutputFields() == null) {
          outputFields.addAll(collection.getOutputFields());
        }
      }

      outputFieldsIndex = new int[outputFields.size()];
      isOutputAllFields = new boolean[outputFields.size()];
      for (int i = 0; i < outputFields.size(); i++) {
        int index = fields.indexOf(outputFields.get(i));
        if (index >= 0) {
          outputFieldsIndex[i] = index;
          if (outputFields.get(i).getFieldName().endsWith(":*")) {
            isOutputAllFields[i] = true;
          }
        }
      }
    }

    List<Object> selectValues = Lists.newArrayList();
    for (int i = 0; i < outputFieldsIndex.length; i++) {
      if (isOutputAllFields[i]) {
        selectValues.addAll((List<Object>) values.get(outputFieldsIndex[i]));
      } else {
        selectValues.add(values.get(outputFieldsIndex[i]));
      }
    }

    outputHandler.output(selectValues);
  }

  private void join(List<List<List<Object>>> joinValues, int index, List<Object> values) {
    for (int i = 0; i < joinValues.get(index).size(); i++) {
      List<Object> newValues;
      if (values != null) {
        newValues = Lists.newArrayList(values);
      } else {
        newValues = Lists.newArrayList();
      }
      newValues.addAll(joinValues.get(index).get(i));

      if (index < collections.size() - 1) {
        join(joinValues, index + 1, newValues);
      } else {
        output(newValues);
      }
    }
  }

  public void join(Object keyValue) {
    for (TupleCollection collection : collections) {
      collection.removeExpired(keyValue);
      if (collection.getTupleStore().count(Query.builder().hashKeyValue(keyValue).build()) == 0) {
        return;
      }
    }

    List<List<List<Object>>> joinValues = Lists.newArrayListWithCapacity(collections.size());
    for (TupleCollection collection : collections) {
      joinValues.add(
          collection.getTupleStore().findAndRemove(Query.builder().hashKeyValue(keyValue).build()));
    }

    join(joinValues, 0, null);
  }

  @Override
  public void close() {
    super.close();
    for (TupleCollection collection : collections) {
      collection.close();
    }
  }

  @Override
  public GroupedTupleCollection clone() {
    return new GroupedTupleCollection(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    for (int i = 0; i < collections.size(); i++) {
      if (i > 0) {
        sb.append(" JOIN ");
      }
      sb.append(collections.get(i));
      sb.append('(');
      sb.append(collections.get(i).getJoinKey());
      sb.append(')');
    }
    sb.append(')');
    return sb.toString();
  }
}
