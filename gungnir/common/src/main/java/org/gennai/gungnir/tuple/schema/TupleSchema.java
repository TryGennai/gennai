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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TupleSchema extends BaseSchema {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public static class FieldTypes {

    public static final PrimitiveType STRING = PrimitiveType.STRING;
    public static final PrimitiveType TINYINT = PrimitiveType.TINYINT;
    public static final PrimitiveType SMALLINT = PrimitiveType.SMALLINT;
    public static final PrimitiveType INT = PrimitiveType.INT;
    public static final PrimitiveType BIGINT = PrimitiveType.BIGINT;
    public static final PrimitiveType FLOAT = PrimitiveType.FLOAT;
    public static final PrimitiveType DOUBLE = PrimitiveType.DOUBLE;
    public static final PrimitiveType BOOLEAN = PrimitiveType.BOOLEAN;

    public static final TimestampType TIMESTAMP = new TimestampType();

    // CHECKSTYLE IGNORE MethodName FOR NEXT 1 LINES
    public static TimestampType TIMESTAMP(String dateFormat) {
      return new TimestampType(dateFormat);
    }

    // CHECKSTYLE IGNORE MethodName FOR NEXT 1 LINES
    public static ListType LIST(FieldType elementType) {
      return new ListType(elementType);
    }

    // CHECKSTYLE IGNORE MethodName FOR NEXT 1 LINES
    public static MapType MAP(FieldType keyType, FieldType valueType) {
      return new MapType(keyType, valueType);
    }

    // CHECKSTYLE IGNORE MethodName FOR NEXT 1 LINES
    public static StructType STRUCT() {
      return new StructType();
    }
  }

  private String tupleName;
  private List<String> fieldNames = Lists.newArrayList();
  private List<FieldType> fieldTypes = Lists.newArrayList();
  private String[] partitionFields;
  private transient Map<String, Integer> fieldsIndex;

  public TupleSchema(String tupleName) {
    super();
    this.tupleName = tupleName;
  }

  private TupleSchema(TupleSchema c) {
    super(c);
    this.tupleName = c.tupleName;
    this.fieldNames = Lists.newArrayList(c.fieldNames);
    this.fieldTypes = Lists.newArrayList(c.fieldTypes);
    if (c.partitionFields != null) {
      this.partitionFields = Arrays.copyOf(c.partitionFields, c.partitionFields.length);
    }
  }

  @Override
  public String getSchemaName() {
    return tupleName;
  }

  @Override
  public int getFieldCount() {
    return fieldTypes.size();
  }

  private void indexing() {
    if (fieldsIndex == null) {
      fieldsIndex = Maps.newHashMap();
      for (int i = 0; i < fieldNames.size(); i++) {
        fieldsIndex.put(fieldNames.get(i), i);
      }
    }
  }

  @Override
  public Integer getFieldIndex(String fieldName) {
    indexing();
    return fieldsIndex.get(fieldName);
  }

  @Override
  public String getFieldName(int index) {
    return fieldNames.get(index);
  }

  @Override
  public FieldType getFieldType(int index) {
    return fieldTypes.get(index);
  }

  @Override
  public List<String> getFieldNames() {
    return fieldNames;
  }

  @Override
  public String[] getPartitionFields() {
    return partitionFields;
  }

  public TupleSchema field(String fieldName, FieldType fieldType) {
    indexing();
    if (fieldsIndex.containsKey(fieldName)) {
      fieldNames.set(fieldsIndex.get(fieldName), fieldName);
      fieldTypes.set(fieldsIndex.get(fieldName), fieldType);
    } else {
      Integer index = fieldTypes.size();
      fieldsIndex.put(fieldName, index);
      fieldNames.add(fieldName);
      fieldTypes.add(fieldType);
    }
    return this;
  }

  public TupleSchema field(String fieldName) {
    return field(fieldName, null);
  }

  public TupleSchema partitioned(String... fieldNames) {
    partitionFields = fieldNames;
    return this;
  }

  @Override
  public void validate() throws SchemaValidateException {
    if (partitionFields != null) {
      for (String fieldName : partitionFields) {
        if (!fieldNames.contains(fieldName)) {
          throw new SchemaValidateException("'" + fieldName + "' field doesn't exist");
        }
      }
    }
  }

  @Override
  public TupleSchema clone() {
    return new TupleSchema(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(tupleName);
    sb.append('(');
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fieldNames.get(i));
      if (fieldTypes.get(i) != null) {
        sb.append(' ');
        sb.append(fieldTypes.get(i));
      }
    }
    sb.append(')');
    if (partitionFields != null) {
      sb.append(" partitioned by ");
      for (int i = 0; i < partitionFields.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(partitionFields[i]);
      }
    }
    return sb.toString();
  }
}
