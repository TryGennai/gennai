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

import java.util.Collections;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;

import com.google.common.collect.Lists;

public class GlobalGrouping extends BaseGrouping {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<Integer> targetTasks;

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    List<Integer> sorted = Lists.newArrayList(targetTasks);
    Collections.sort(sorted);
    this.targetTasks = Lists.newArrayListWithCapacity(1);
    this.targetTasks.add(sorted.get(0));
  }

  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    return targetTasks;
  }

  @Override
  public int hashCode() {
    return 1;
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
    return true;
  }

  @Override
  public String toString() {
    return "global grouping";
  }
}
