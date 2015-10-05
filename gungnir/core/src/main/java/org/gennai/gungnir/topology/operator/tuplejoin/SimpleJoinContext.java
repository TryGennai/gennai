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

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;

import com.google.common.collect.Lists;

public class SimpleJoinContext extends BaseJoinContext {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private TupleAccessor fromTuple;
  private List<FieldAccessor> fields;
  private JoinKey joinKey;

  public SimpleJoinContext(TupleAccessor fromTuple, List<FieldAccessor> fields) {
    this.fromTuple = fromTuple;
    this.fields = fields;
  }

  private SimpleJoinContext(SimpleJoinContext c) {
    this.fromTuple = c.fromTuple;
    this.fields = c.fields;
    this.joinKey = c.joinKey;
  }

  @Override
  public TupleAccessor getFromTuple() {
    return fromTuple;
  }

  @Override
  public List<FieldAccessor> getFields() {
    return fields;
  }

  @Override
  public void setJoinKey(JoinKey joinKey) {
    this.joinKey = joinKey;
  }

  @Override
  public JoinKey getJoinKey() {
    return joinKey;
  }

  @Override
  public List<FieldAccessor> getOutputFields() {
    List<FieldAccessor> outputFields = Lists.newArrayListWithCapacity(fields.size());
    for (FieldAccessor field : fields) {
      outputFields.add(getJoinField(field));
    }
    return outputFields;
  }

  @Override
  public Object getKey(GungnirTuple tuple) {
    Object keyValue = null;
    if (joinKey instanceof ComplexJoinKey) {
      List<Object> keyValues = Lists.newArrayListWithCapacity(((ComplexJoinKey) joinKey)
          .getJoinKeys().size());
      for (SimpleJoinKey simpleKey : ((ComplexJoinKey) joinKey).getJoinKeys()) {
        keyValues.add(simpleKey.getKeyField().getValue(tuple));
      }
      keyValue = keyValues;
    } else {
      keyValue = ((SimpleJoinKey) joinKey).getKeyField().getValue(tuple);
    }
    return keyValue;
  }

  public List<Object> getValues(GungnirTuple tuple) {
    List<Object> values = Lists.newArrayList();
    for (FieldAccessor field : fields) {
      if (field.isWildcardField()) {
        values.add(tuple.getTupleValues().getValues());
      } else {
        values.add(field.getValue(tuple));
      }
    }
    return values;
  }

  @Override
  public String toString() {
    return fromTuple.toString();
  }

  @Override
  public SimpleJoinContext clone() {
    return new SimpleJoinContext(this);
  }
}
