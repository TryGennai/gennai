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

package org.gennai.gungnir.tuple.schema;

import static org.gennai.gungnir.GungnirConst.*;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.Struct;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class StructType implements FieldType {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Map<String, Integer> indexMap = Maps.newHashMap();
  private List<String> fieldNames = Lists.newArrayList();
  private List<FieldType> fieldTypes = Lists.newArrayList();

  public StructType field(String fieldName) {
    return field(fieldName, null);
  }

  public int getFieldCount() {
    return fieldTypes.size();
  }

  public Integer getFieldIndex(String fieldName) {
    return indexMap.get(fieldName);
  }

  public String getFieldName(int index) {
    return fieldNames.get(index);
  }

  public FieldType getFieldType(int index) {
    return fieldTypes.get(index);
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public StructType field(String fieldName, FieldType type) {
    if (indexMap.containsKey(fieldName)) {
      fieldTypes.set(indexMap.get(fieldName), type);
    } else {
      Integer index = fieldTypes.size();
      indexMap.put(fieldName, index);
      fieldNames.add(fieldName);
      fieldTypes.add(index, type);
    }
    return this;
  }

  @Override
  public String getName() {
    return TypeDef.STRUCT.name();
  }

  @Override
  public Type getJavaType() {
    return TypeDef.STRUCT.getJavaType();
  }

  public boolean isInstance(Object obj) {
    if (!(obj instanceof Struct)) {
      return false;
    }
    Struct struct = (Struct) obj;
    if (!struct.getFieldNames().equals(fieldNames)) {
      return false;
    }
    if (struct.getValues().size() != fieldNames.size()) {
      return false;
    }
    for (int i = 0; i < fieldTypes.size(); i++) {
      if (fieldTypes.get(i) != null && struct.getValues().get(i) != null) {
        if (!fieldTypes.get(i).isInstance(struct.getValues().get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldNames == null) ? 0 : fieldNames.hashCode());
    result = prime * result + ((fieldTypes == null) ? 0 : fieldTypes.hashCode());
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
    StructType other = (StructType) obj;
    if (fieldNames == null) {
      if (other.fieldNames != null) {
        return false;
      }
    } else if (!fieldNames.equals(other.fieldNames)) {
      return false;
    }
    if (fieldTypes == null) {
      if (other.fieldTypes != null) {
        return false;
      }
    } else if (!fieldTypes.equals(other.fieldTypes)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getName());
    sb.append('<');
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fieldNames.get(i));
      sb.append(' ');
      sb.append(fieldTypes.get(i));
    }
    sb.append('>');
    return sb.toString();
  }
}
