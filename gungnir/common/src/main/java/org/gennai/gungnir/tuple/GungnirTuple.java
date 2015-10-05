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

package org.gennai.gungnir.tuple;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.tuple.schema.Schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class GungnirTuple {

  private List<String> fieldNames;
  private TupleValues tupleValues;
  private Map<String, Integer> fieldsIndex;

  public GungnirTuple(List<String> fieldNames, TupleValues tupleValues) {
    this.fieldNames = fieldNames;
    this.tupleValues = tupleValues;
  }

  public GungnirTuple(List<String> fieldNames) {
    this.fieldNames = fieldNames;
  }

  public static final class Builder {

    private Schema schema;
    private Map<String, Object> fieldsMap;

    private Builder(Schema schema) {
      this.schema = schema;
      fieldsMap = Maps.newHashMap();
    }

    public Builder put(String fieldName, Object value) {
      fieldsMap.put(fieldName, value);
      return this;
    }

    public GungnirTuple build() throws InvalidTupleException {
      List<Object> values = Lists.newArrayListWithCapacity(schema.getFieldCount());
      for (int i = 0; i < schema.getFieldCount(); i++) {
        Object value = fieldsMap.remove(schema.getFieldName(i));
        if (schema.getFieldType(i) != null && value != null
            && !schema.getFieldType(i).isInstance(value)) {
          throw new InvalidTupleException("Field " + schema.getFieldName(i) + " must be "
              + schema.getFieldType(i));
        }
        values.add(value);
      }

      if (!fieldsMap.isEmpty()) {
        throw new InvalidTupleException(StringUtils.join(fieldsMap.keySet(), ", ")
            + " fields is undefined");
      }

      TupleValues tupleValues = new TupleValues(schema.getSchemaName(), values);
      return new GungnirTuple(schema.getFieldNames(), tupleValues);
    }
  }

  public static Builder builder(Schema schema) {
    return new Builder(schema);
  }

  public String getTupleName() {
    return tupleValues.getTupleName();
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public void setTupleValues(TupleValues tupleValues) {
    this.tupleValues = tupleValues;
  }

  public TupleValues getTupleValues() {
    return tupleValues;
  }

  public Integer fieldIndex(String fieldName) {
    if (fieldsIndex == null) {
      fieldsIndex = Maps.newHashMap();
      for (int i = 0; i < fieldNames.size(); i++) {
        fieldsIndex.put(fieldNames.get(i), i);
      }
    }
    return fieldsIndex.get(fieldName);
  }

  public Object getValueByField(String fieldName) {
    Integer index = fieldIndex(fieldName);
    if (index != null) {
      return tupleValues.getValues().get(fieldIndex(fieldName));
    } else {
      return null;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldNames == null) ? 0 : fieldNames.hashCode());
    result = prime * result + ((tupleValues == null) ? 0 : tupleValues.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    GungnirTuple other = (GungnirTuple) obj;
    if (fieldNames == null) {
      if (other.fieldNames != null) {
        return false;
      }
    } else if (!fieldNames.equals(other.fieldNames)) {
      return false;
    }
    if (tupleValues == null) {
      if (other.tupleValues != null) {
        return false;
      }
    } else if (!tupleValues.equals(other.tupleValues)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(tupleValues.getTupleName());
    sb.append('(');
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fieldNames.get(i));
      sb.append('=');
      sb.append(tupleValues.getValues().get(i));
    }
    sb.append(')');
    return sb.toString();
  }
}
