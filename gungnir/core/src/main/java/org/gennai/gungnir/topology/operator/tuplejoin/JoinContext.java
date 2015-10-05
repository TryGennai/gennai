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

package org.gennai.gungnir.topology.operator.tuplejoin;

import java.io.Serializable;
import java.util.List;

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;

public interface JoinContext extends Serializable, Cloneable {

  TupleAccessor getFromTuple();

  List<FieldAccessor> getFields();

  void setJoinKey(JoinKey joinKey);

  JoinKey getJoinKey();

  List<FieldAccessor> getOutputFields();

  Object getKey(GungnirTuple tuple);

  List<Object> getValues(GungnirTuple tuple);
}
