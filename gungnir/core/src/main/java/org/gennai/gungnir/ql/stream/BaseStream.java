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

package org.gennai.gungnir.ql.stream;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.topology.operator.Operator;
import org.gennai.gungnir.tuple.TupleAccessor;

public abstract class BaseStream implements Stream {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private GungnirTopology topology;
  private Operator source;
  private TupleAccessor[] selector;

  protected BaseStream(GungnirTopology topology, Operator source) {
    this.topology = topology;
    this.source = source;
  }

  protected BaseStream(GungnirTopology topology, Operator source,
      TupleAccessor[] selector) {
    this.topology = topology;
    this.source = source;
    this.selector = selector;
  }

  @Override
  public GungnirTopology getTopology() {
    return topology;
  }

  @Override
  public Operator getSource() {
    return source;
  }

  @Override
  public TupleAccessor[] getSelector() {
    return selector;
  }

  protected abstract Stream addOperator(Operator target);

  protected abstract Stream addOperator(Operator target, TupleAccessor... selector);
}
