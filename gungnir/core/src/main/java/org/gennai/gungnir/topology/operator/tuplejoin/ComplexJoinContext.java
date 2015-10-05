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

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ComplexJoinContext extends BaseJoinContext {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(ComplexJoinContext.class);

  private List<JoinContext> contexts = Lists.newArrayList();
  private List<FieldAccessor> fields = Lists.newArrayList();
  private JoinKey joinKey;
  private Map<String, Integer> contextsIndexMap;
  private List<Integer> keyFieldsIndex;

  public ComplexJoinContext() {
  }

  private ComplexJoinContext(ComplexJoinContext c) {
    this.contexts = c.contexts;
    this.fields = c.fields;
    this.joinKey = c.joinKey;
  }

  public void addContext(JoinContext joinContext) {
    contexts.add(joinContext);
    fields.addAll(joinContext.getOutputFields());
  }

  public List<JoinContext> getContexts() {
    return contexts;
  }

  public JoinContext getContext(String tupleName) {
    if (contextsIndexMap == null) {
      contextsIndexMap = Maps.newHashMap();
      for (int i = 0; i < contexts.size(); i++) {
        contextsIndexMap.put(contexts.get(i).getFromTuple().getTupleName(), i);
      }
    }
    return contexts.get(contextsIndexMap.get(tupleName));
  }

  @Override
  public TupleAccessor getFromTuple() {
    if (contexts.size() > 0) {
      return contexts.get(0).getFromTuple();
    } else {
      return null;
    }
  }

  @Override
  public List<FieldAccessor> getFields() {
    return fields;
  }

  private void addKeyFields(FieldAccessor keyField) {
    int index = fields.indexOf(keyField);
    if (index >= 0) {
      keyFieldsIndex.add(index);
    } else {
      keyFieldsIndex.add(fields.size());
      fields.add(keyField);
    }
  }

  private void setKeyFieldsIndex() {
    keyFieldsIndex = Lists.newArrayList();

    if (joinKey instanceof ComplexJoinKey) {
      for (SimpleJoinKey simpleKey : ((ComplexJoinKey) joinKey).getJoinKeys()) {
        addKeyFields(getJoinField(simpleKey.getKeyField()));
      }
    } else {
      addKeyFields(getJoinField(((SimpleJoinKey) joinKey).getKeyField()));
    }

    for (JoinContext context : contexts) {
      if (context.getFromTuple().getTupleName().equals(joinKey.getTupleName())) {
        if (joinKey instanceof ComplexJoinKey) {
          for (SimpleJoinKey simpleKey : ((ComplexJoinKey) joinKey).getJoinKeys()) {
            if (!context.getFields().contains(simpleKey.getKeyField())) {
              context.getFields().add(simpleKey.getKeyField());
              context.getOutputFields().add(getJoinField(simpleKey.getKeyField()));
            }
          }
        } else {
          if (!context.getFields().contains(((SimpleJoinKey) joinKey).getKeyField())) {
            context.getFields().add(((SimpleJoinKey) joinKey).getKeyField());
            context.getOutputFields().add(getJoinField(((SimpleJoinKey) joinKey).getKeyField()));
          }
        }
      }
    }
  }

  @Override
  public void setJoinKey(JoinKey joinKey) {
    this.joinKey = joinKey;
    setKeyFieldsIndex();
  }

  @Override
  public JoinKey getJoinKey() {
    return joinKey;
  }

  @Override
  public List<FieldAccessor> getOutputFields() {
    return fields;
  }

  public int[] getFieldsIndex(List<FieldAccessor> targetFields) {
    List<FieldAccessor> targets = Lists.newArrayListWithCapacity(targetFields.size());
    for (FieldAccessor field : targetFields) {
      targets.add(getJoinField(field));
    }

    int[] outputFieldsIndex = new int[targets.size()];
    for (int i = 0; i < targets.size(); i++) {
      int index = fields.indexOf(targets.get(i));
      if (index >= 0) {
        outputFieldsIndex[i] = index;
      } else {
        outputFieldsIndex[i] = -1;
      }
    }
    return outputFieldsIndex;
  }

  @Override
  public Object getKey(GungnirTuple tuple) {
    if (keyFieldsIndex == null) {
      setKeyFieldsIndex();
    }

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
  public List<Object> getValues(GungnirTuple tuple) {
    return tuple.getTupleValues().getValues();
  }

  @Override
  public ComplexJoinContext clone() {
    return new ComplexJoinContext(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    for (int i = 0; i < contexts.size(); i++) {
      if (i > 0) {
        sb.append(" JOIN ");
      }
      sb.append(contexts.get(i));
      sb.append('(');
      sb.append(contexts.get(i).getJoinKey());
      sb.append(')');
    }
    sb.append(')');
    return sb.toString();
  }
}
