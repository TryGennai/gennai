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

import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Operator.Description(name = "RENAME", parameterNames = {"fromTuple", "toTuple"})
public class RenameOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(RenameOperator.class);

  private TupleAccessor fromTuple;
  private TupleAccessor toTuple;

  public RenameOperator(TupleAccessor fromTuple, TupleAccessor toTuple) {
    this.fromTuple = fromTuple;
    this.toTuple = toTuple;
  }

  private RenameOperator(RenameOperator c) {
    super(c);
    this.fromTuple = c.fromTuple;
    this.toTuple = c.toTuple;
  }

  public TupleAccessor getFromTuple() {
    return fromTuple;
  }

  public TupleAccessor getToTuple() {
    return toTuple;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    tuple.getTupleValues().setTupleName(toTuple.getTupleName());
    dispatch(tuple.getTupleValues());
  }

  @Override
  public RenameOperator clone() {
    return new RenameOperator(this);
  }
}
