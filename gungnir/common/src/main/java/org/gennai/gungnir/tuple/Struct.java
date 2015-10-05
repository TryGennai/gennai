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
import org.gennai.gungnir.tuple.schema.StructType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Struct {

  private List<String> fieldNames;
  private List<Object> values;
  private Map<String, Integer> fieldsIndex;

  public Struct() {
  }

  public Struct(List<String> fieldNames, List<Object> values) {
    this.fieldNames = fieldNames;
    this.values = values;
  }

  public static final class Builder {

    private StructType fieldType;
    private Map<String, Object> fieldsMap;

    private Builder(StructType fieldType) {
      this.fieldType = fieldType;
      fieldsMap = Maps.newHashMap();
    }

    public Builder put(String fieldName, Object value) {
      fieldsMap.put(fieldName, value);
      return this;
    }

    public Struct build() throws InvalidTupleException {
      List<Object> values = Lists.newArrayListWithCapacity(fieldType.getFieldCount());
      for (int i = 0; i < fieldType.getFieldCount(); i++) {
        Object value = fieldsMap.remove(fieldType.getFieldName(i));
        if (fieldType.getFieldType(i) != null && value != null
            && !fieldType.getFieldType(i).isInstance(value)) {
          throw new InvalidTupleException("Field " + fieldType.getFieldName(i) + " must be "
              + fieldType.getFieldType(i));
        }
        values.add(value);
      }

      if (!fieldsMap.isEmpty()) {
        throw new InvalidTupleException(StringUtils.join(fieldsMap.keySet(), ", ")
            + " fields is undefined");
      }

      return new Struct(fieldType.getFieldNames(), values);
    }
  }

  public static Builder builder(StructType fieldType) {
    return new Builder(fieldType);
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public List<Object> getValues() {
    return values;
  }

  public Integer fieldIndex(String fieldName) {
    if (fieldsIndex == null) {
      fieldsIndex = Maps.newHashMap();
      for (int i = 0; i < fieldNames.size(); i++) {
        this.fieldsIndex.put(fieldNames.get(i), i);
      }
    }
    return fieldsIndex.get(fieldName);
  }

  public Object getValueByField(String fieldName) {
    Integer index = fieldIndex(fieldName);
    if (index != null) {
      return values.get(index);
    } else {
      return null;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldNames == null) ? 0 : fieldNames.hashCode());
    result = prime * result + ((values == null) ? 0 : values.hashCode());
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
    Struct other = (Struct) obj;
    if (fieldNames == null) {
      if (other.fieldNames != null) {
        return false;
      }
    } else if (!fieldNames.equals(other.fieldNames)) {
      return false;
    }
    if (values == null) {
      if (other.values != null) {
        return false;
      }
    } else if (!values.equals(other.values)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fieldNames.get(i));
      sb.append('=');
      sb.append(values.get(i));
    }
    sb.append(')');
    return sb.toString();
  }
}
