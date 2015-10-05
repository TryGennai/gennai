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

import java.util.Set;

import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.TupleValues;

import com.google.common.collect.Sets;

public class TupleNameFilter implements DispatchFilter {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Set<String> tupleNames;

  public TupleNameFilter(TupleAccessor[] tuples) {
    tupleNames = Sets.newLinkedHashSet();
    for (TupleAccessor tuple : tuples) {
      tupleNames.add(tuple.getTupleName());
    }
  }

  @Override
  public void doFilter(TupleValues tupleValues, Dispatcher dispatcher) {
    if (tupleNames.contains(tupleValues.getTupleName())) {
      dispatcher.dispatch(tupleValues);
    }
  }

  @Override
  public String toString() {
    return "TupleNameFilter(tupleName=" + tupleNames + ")";
  }
}
