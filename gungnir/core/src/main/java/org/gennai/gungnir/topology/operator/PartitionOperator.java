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

package org.gennai.gungnir.topology.operator;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.topology.grouping.Grouping;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Operator.Description(name = "PARTITION", parameterNames = "grouping")
public class PartitionOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private static final Logger LOG = LoggerFactory.getLogger(PartitionOperator.class);

  private Grouping grouping;

  public PartitionOperator(Grouping grouping) {
    super();
    this.grouping = grouping;
  }

  private PartitionOperator(PartitionOperator c) {
    super(c);
    this.grouping = c.grouping;
  }

  public void setGrouping(Grouping grouping) {
    this.grouping = grouping;
  }

  public Grouping getGrouping() {
    return grouping;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    dispatch(tuple.getTupleValues());
  }

  @Override
  public PartitionOperator clone() {
    return new PartitionOperator(this);
  }
}
