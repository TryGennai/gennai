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
import java.util.Map;

import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.tuple.TupleValues;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Maps;

public class SelectGrouping extends BaseGrouping {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Map<String, Grouping> groupingsMap = Maps.newLinkedHashMap();
  private Fields outputFields;

  public void addGrouping(String tupleName, Grouping grouping) {
    groupingsMap.put(tupleName, grouping);
  }

  @Override
  public void setContext(GungnirContext context) {
    super.setContext(context);
    for (Grouping grouping : groupingsMap.values()) {
      grouping.setContext(context);
    }
  }

  @Override
  public void setPartitionName(String partitionName) {
    super.setPartitionName(partitionName);
    for (Grouping grouping : groupingsMap.values()) {
      grouping.setPartitionName(partitionName);
    }
  }

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    for (Grouping grouping : groupingsMap.values()) {
      grouping.prepare(context, stream, targetTasks);
    }
    this.outputFields = context.getComponentOutputFields(stream);
  }

  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    TupleValues tupleValues = (TupleValues) values.get(outputFields.fieldIndex(TUPLE_FIELD));
    Grouping grouping = groupingsMap.get(tupleValues.getTupleName());
    return grouping.chooseTasks(taskId, values);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((groupingsMap == null) ? 0 : groupingsMap.hashCode());
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
    SelectGrouping other = (SelectGrouping) obj;
    if (groupingsMap == null) {
      if (other.groupingsMap != null) {
        return false;
      }
    } else if (!groupingsMap.equals(other.groupingsMap)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Grouping> entry : groupingsMap.entrySet()) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(entry.getKey());
      sb.append('(');
      sb.append(entry.getValue());
      sb.append(')');
    }
    return sb.toString();
  }
}
