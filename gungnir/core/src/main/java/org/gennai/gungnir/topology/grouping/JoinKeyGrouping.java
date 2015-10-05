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

import org.gennai.gungnir.topology.operator.tuplejoin.ComplexJoinContext;
import org.gennai.gungnir.topology.operator.tuplejoin.JoinContext;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JoinKeyGrouping extends BaseGrouping {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private static class GroupingContext {

    private JoinContext joinContext;
    private GungnirTuple tuple;
  }

  private ComplexJoinContext complexContext;
  private Map<String, GroupingContext> contextsMap;
  private List<Integer> targetTasks;
  private Fields outputFields;

  public JoinKeyGrouping(ComplexJoinContext complexContext) {
    this.complexContext = complexContext;
  }

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    contextsMap = Maps.newHashMap();
    for (JoinContext joinContext : complexContext.getContexts()) {
      GroupingContext groupingContext = new GroupingContext();
      groupingContext.joinContext = joinContext;
      contextsMap.put(joinContext.getFromTuple().getTupleName(), groupingContext);
    }
    this.targetTasks = targetTasks;
    this.outputFields = context.getComponentOutputFields(stream);
  }

  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    List<Integer> tasks = Lists.newArrayListWithCapacity(1);

    TupleValues tupleValues = (TupleValues) values.get(outputFields.fieldIndex(TUPLE_FIELD));

    GroupingContext groupingContext = contextsMap.get(tupleValues.getTupleName());
    if (groupingContext.tuple == null) {
      groupingContext.tuple = new GungnirTuple(getContext().getOutputFields()
          .get(getPartitionName()).get(tupleValues.getTupleName()));
    }
    groupingContext.tuple.setTupleValues(tupleValues);

    tasks.add(targetTasks.get(hashIndex(groupingContext.joinContext.getKey(groupingContext.tuple),
        targetTasks.size())));

    return tasks;
  }

  @Override
  public String toString() {
    return "join key grouping" + complexContext.toString();
  }
}
