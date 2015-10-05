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

package org.gennai.gungnir.topology.dispatcher;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.topology.operator.ExecOperator;
import org.gennai.gungnir.topology.operator.Operator;
import org.gennai.gungnir.topology.operator.metrics.MultiCountMeter;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class SingleDispatcher extends BaseDispatcher {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Operator target;

  private Marker marker;
  private MultiCountMeter dispatchCount;

  public SingleDispatcher(Operator target) {
    this.target = target;
  }

  @Override
  protected void prepare() {
    marker = MarkerFactory.getMarker(PATH_MARKER_NAME + " " + getContext().getTopologyName());
    if (getConfig().getBoolean(TOPOLOGY_METRICS_ENABLED)) {
      dispatchCount = getSource().getMetrics(METRICS_DISPATCH_COUNT);
    }
  }

  @Override
  public void dispatch(TupleValues tupleValues) {
    if (target instanceof ExecOperator) {
      if (!target.isPrepared()) {
        target.doPrepare(getConfig(), getContext());
      }

      GungnirTuple tuple =
          new GungnirTuple(getContext().getOutputFields().get(getSource().getName())
              .get(tupleValues.getTupleName()), tupleValues);

      getDebugLogger().logging(marker, getSource(), target, tuple);
      if (dispatchCount != null) {
        dispatchCount.scope(target.getName()).mark();
      }

      ((ExecOperator) target).execute(tuple);
    }
  }

  @Override
  public void cleanup() {
    if (target.isPrepared() && !target.isCleanedup()) {
      target.doCleanup();
    }
  }

  @Override
  public String toString() {
    return "-SingleDispatcher-> " + target.getName();
  }
}
