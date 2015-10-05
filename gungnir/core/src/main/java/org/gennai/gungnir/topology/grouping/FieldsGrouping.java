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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Lists;

public class FieldsGrouping extends BaseGrouping {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private GroupFields groupFields;
  private List<Integer> targetTasks;
  private Fields outputFields;

  public FieldsGrouping(GroupFields groupFields) {
    this.groupFields = groupFields;
  }

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    this.targetTasks = targetTasks;
    this.outputFields = context.getComponentOutputFields(stream);
  }

  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    List<Integer> tasks = Lists.newArrayListWithCapacity(1);

    TupleValues tupleValues = (TupleValues) values.get(outputFields.fieldIndex(TUPLE_FIELD));
    GungnirTuple tuple = new GungnirTuple(getContext().getOutputFields().get(getPartitionName())
        .get(tupleValues.getTupleName()), tupleValues);

    List<Object> key = Lists.newArrayListWithCapacity(groupFields.getFields().length);
    for (FieldAccessor field : groupFields.getFields()) {
      key.add(field.getValue(tuple));
    }
    tasks.add(targetTasks.get(hashIndex(key, targetTasks.size())));

    return tasks;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((groupFields == null) ? 0 : groupFields.hashCode());
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
    FieldsGrouping other = (FieldsGrouping) obj;
    if (groupFields == null) {
      if (other.groupFields != null) {
        return false;
      }
    } else if (!groupFields.equals(other.groupFields)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "fields grouping(" + groupFields.toString() + ")";
  }
}
