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

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.store.Query;
import org.gennai.gungnir.tuple.store.TupleStore;
import org.gennai.gungnir.tuple.store.Query.ConditionType;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public abstract class BaseTupleCollection implements TupleCollection {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(BaseTupleCollection.class);

  private List<FieldAccessor> fields;
  private List<FieldAccessor> outputFields;
  private TupleStore tupleStore;
  private JoinKey joinKey;
  private List<Integer> keyFieldsIndex;

  public BaseTupleCollection() {
    fields = Lists.newArrayList();
  }

  public BaseTupleCollection(List<FieldAccessor> fields) {
    this.fields = fields;
  }

  public BaseTupleCollection(BaseTupleCollection c) {
    this.fields = c.fields;
    this.outputFields = c.outputFields;
    this.tupleStore = c.tupleStore;
    this.joinKey = c.joinKey;
    this.keyFieldsIndex = c.keyFieldsIndex;
  }

  @Override
  public List<FieldAccessor> getFields() {
    return fields;
  }

  protected FieldAccessor getJoinField(FieldAccessor field) {
    if (field.getParentAccessor() != null) {
      FieldAccessor parent = getJoinField(field.getParentAccessor());
      if (field.getOriginalName().equals(field.getFieldName())) {
        return new FieldAccessor(field.getOriginalName(), parent);
      } else {
        return new FieldAccessor(field.getOriginalName(), parent).as(field.getFieldName());
      }
    } else {
      String fieldName = null;
      if (field.getTupleAccessor() != null) {
        fieldName = "+" + field.getTupleAccessor().getTupleName() + ":" + field.getOriginalName();
      } else {
        fieldName = field.getOriginalName();
      }
      if (field.getOriginalName().equals(field.getFieldName())) {
        return new FieldAccessor(fieldName);
      } else {
        return new FieldAccessor(fieldName).as(field.getFieldName());
      }
    }
  }

  @Override
  public void setOutputFields(List<FieldAccessor> outputFields) {
    this.outputFields = Lists.newArrayListWithCapacity(outputFields.size());
    for (FieldAccessor field : outputFields) {
      this.outputFields.add(getJoinField(field));
    }
  }

  @Override
  public List<FieldAccessor> getOutputFields() {
    return outputFields;
  }

  @Override
  public void setTupleStore(TupleStore tupleStore) {
    this.tupleStore = tupleStore;
  }

  @Override
  public TupleStore getTupleStore() {
    return this.tupleStore;
  }

  @Override
  public void setJoinKey(JoinKey joinKey) {
    this.joinKey = joinKey;
    keyFieldsIndex = Lists.newArrayList();
    if (joinKey instanceof ComplexJoinKey) {
      for (SimpleJoinKey simpleKey : ((ComplexJoinKey) joinKey).getJoinKeys()) {
        FieldAccessor keyField = simpleKey.getKeyField();
        int index = fields.indexOf(keyField);
        if (index >= 0) {
          keyFieldsIndex.add(index);
        } else {
          keyFieldsIndex.add(fields.size());
          if (this instanceof GroupedTupleCollection) {
            fields.add(getJoinField(keyField));
          } else {
            fields.add(keyField);
          }
        }
      }
    } else {
      FieldAccessor keyField = ((SimpleJoinKey) joinKey).getKeyField();
      int index = fields.indexOf(keyField);
      if (index >= 0) {
        keyFieldsIndex.add(index);
      } else {
        keyFieldsIndex.add(fields.size());
        if (this instanceof GroupedTupleCollection) {
          fields.add(getJoinField(keyField));
        } else {
          fields.add(keyField);
        }
      }
    }
  }

  @Override
  public JoinKey getJoinKey() {
    return joinKey;
  }

  @Override
  public Object getKey(List<Object> values) {
    Object keyValue = null;
    if (keyFieldsIndex.size() > 1) {
      List<Object> keyValues = Lists.newArrayListWithCapacity(keyFieldsIndex.size());
      for (Integer index : keyFieldsIndex) {
        Object value = values.get(index);
        if (value == null) {
          keyValues = null;
          break;
        }
        keyValues.add(value);
      }
      keyValue = keyValues;
    } else {
      keyValue = values.get(keyFieldsIndex.get(0));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("getKey: {}, collection: {}, values: {}", keyValue, toString(), values);
    }
    return keyValue;
  }

  @Override
  public Object getKey(GungnirTuple tuple) {
    Object keyValue = null;
    if (keyFieldsIndex.size() > 1) {
      List<Object> keyValues = Lists.newArrayListWithCapacity(keyFieldsIndex.size());
      for (int index : keyFieldsIndex) {
        keyValues.add(fields.get(index).getValue(tuple));
      }
      keyValue = keyValues;
    } else {
      keyValue = fields.get(keyFieldsIndex.get(0)).getValue(tuple);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("getKey: {}, collection: {}, tuple: {}, fields: {}", keyValue, toString(), tuple,
          fields);
    }
    return keyValue;
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context) {
    if (tupleStore != null && !tupleStore.isOpen()) {
      tupleStore.open(config, context);
    }
  }

  @Override
  public void add(Object keyValue, List<Object> values, int expire) {
    if (tupleStore != null && tupleStore.isOpen()) {
      tupleStore.put(keyValue, expire, values);
    }
  }

  @Override
  public void removeExpired(Object keyValue) {
    if (tupleStore != null && tupleStore.isOpen()) {
      tupleStore.remove(Query.builder().hashKeyValue(keyValue)
          .timeKeyCondition(ConditionType.LE, GungnirUtils.currentTimeSecs()).build());
    }
  }

  @Override
  public void close() {
    if (tupleStore != null && tupleStore.isOpen()) {
      tupleStore.close();
    }
  }

  @Override
  public abstract BaseTupleCollection clone();
}
