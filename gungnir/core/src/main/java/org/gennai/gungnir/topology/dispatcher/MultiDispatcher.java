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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.tuple.TupleValues;

import com.google.common.collect.Lists;

public class MultiDispatcher extends BaseDispatcher {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<Dispatcher> dispatchers;

  public List<Dispatcher> getDispatchers() {
    return dispatchers;
  }

  public void addDispatcher(Dispatcher dispatcher) {
    if (dispatchers == null) {
      dispatchers = Lists.newArrayList();
    }
    dispatchers.add(dispatcher);
  }

  @Override
  protected void prepare() {
  }

  @Override
  public void dispatch(TupleValues tupleValues) {
    if (dispatchers != null) {
      for (Dispatcher dispatcher : dispatchers) {
        if (!dispatcher.isPrepared()) {
          dispatcher.doPrepare(getConfig(), getContext());
        }

        if (dispatcher instanceof FilterDispatcher) {
          ((FilterDispatcher) dispatcher).clone().dispatch(tupleValues.clone());
        } else {
          dispatcher.dispatch(tupleValues.clone());
        }
      }
    }
  }

  @Override
  public void cleanup() {
    for (Dispatcher dispatcher : dispatchers) {
      if (dispatcher.isPrepared() && !dispatcher.isCleanedup()) {
        dispatcher.doCleanup();
      }
    }
  }

  @Override
  public String toString() {
    return "-MultiDispatcher" + dispatchers.toString();
  }
}
