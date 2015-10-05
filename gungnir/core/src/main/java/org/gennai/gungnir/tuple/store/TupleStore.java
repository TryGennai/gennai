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

package org.gennai.gungnir.tuple.store;

import java.io.Serializable;
import java.util.List;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;

public interface TupleStore extends Serializable {

  void open(GungnirConfig config, GungnirContext context);

  boolean isOpen();

  void put(Object hashKeyValue, int timeKeyValue, List<Object> values);

  int count();

  int count(Query query);

  List<List<Object>> find(Query query);

  List<List<Object>> findAndRemove(Query query);

  void remove(Query query);

  void close();
}
