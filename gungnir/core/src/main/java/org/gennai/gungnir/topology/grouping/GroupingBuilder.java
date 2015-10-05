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

package org.gennai.gungnir.topology.grouping;

import java.util.List;

import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.schema.Schema;

public final class GroupingBuilder {

  private List<Schema> schemas;

  private GroupingBuilder(List<Schema> schemas) {
    this.schemas = schemas;
  }

  public static GroupingBuilder forSchemas(List<Schema> schemas) {
    return new GroupingBuilder(schemas);
  }

  public Grouping build() {
    if (schemas != null) {
      if (schemas.size() > 1) {
        SelectGrouping grouping = new SelectGrouping();
        for (Schema schema : schemas) {
          if (schema.getPartitionFields() != null) {
            FieldAccessor[] groupFields = new FieldAccessor[schema.getPartitionFields().length];
            for (int i = 0; i < groupFields.length; i++) {
              groupFields[i] = new FieldAccessor(schema.getPartitionFields()[i]);
            }
            grouping.addGrouping(schema.getSchemaName(), new FieldsGrouping(new GroupFields(
                groupFields)));
          } else {
            grouping.addGrouping(schema.getSchemaName(), new ShuffleGrouping());
          }
        }
        return grouping;
      } else if (schemas.size() == 1) {
        Schema schema = schemas.get(0);
        if (schemas.get(0).getPartitionFields() != null) {
          FieldAccessor[] groupFields = new FieldAccessor[schema.getPartitionFields().length];
          for (int i = 0; i < groupFields.length; i++) {
            groupFields[i] = new FieldAccessor(schema.getPartitionFields()[i]);
          }
          return new FieldsGrouping(new GroupFields(groupFields));
        } else {
          return new ShuffleGrouping();
        }
      }
    }
    return new ShuffleGrouping();
  }
}
