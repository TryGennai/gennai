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

import java.util.List;

import org.gennai.gungnir.tuple.Condition;

public class ViewSchema extends BaseSchema {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String viewName;
  private TupleSchema tupleSchema;
  private Condition condition;

  public ViewSchema(String viewName) {
    super();
    this.viewName = viewName;
  }

  private ViewSchema(ViewSchema c) {
    super(c);
    this.viewName = c.viewName;
    if (c.tupleSchema != null) {
      this.tupleSchema = c.tupleSchema.clone();
    }
    if (c.condition != null) {
      this.condition = c.condition.clone();
    }
  }

  @Override
  public String getSchemaName() {
    return viewName;
  }

  @Override
  public int getFieldCount() {
    return tupleSchema.getFieldCount();
  }

  @Override
  public Integer getFieldIndex(String fieldName) {
    return tupleSchema.getFieldIndex(fieldName);
  }

  @Override
  public String getFieldName(int index) {
    return tupleSchema.getFieldName(index);
  }

  @Override
  public FieldType getFieldType(int index) {
    return tupleSchema.getFieldType(index);
  }

  @Override
  public List<String> getFieldNames() {
    return tupleSchema.getFieldNames();
  }

  @Override
  public String[] getPartitionFields() {
    return tupleSchema.getPartitionFields();
  }

  public TupleSchema getTupleSchema() {
    return tupleSchema;
  }

  public Condition getCondition() {
    return condition;
  }

  public ViewSchema from(TupleSchema tupleSchema) {
    this.tupleSchema = tupleSchema;
    return this;
  }

  public ViewSchema filter(Condition condition) {
    this.condition = condition;
    return this;
  }

  @Override
  public void validate() throws SchemaValidateException {
    List<String> fieldNames = condition.getFieldNames();
    for (String fieldName : fieldNames) {
      if (!tupleSchema.getFieldNames().contains(fieldName)) {
        throw new SchemaValidateException("'" + fieldName + "' field doesn't exist");
      }
    }
  }

  @Override
  public ViewSchema clone() {
    return new ViewSchema(this);
  }

  @Override
  public String toString() {
    return viewName + " from " + tupleSchema.getSchemaName() + " filter " + condition;
  }
}
