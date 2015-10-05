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

package org.gennai.gungnir.cluster;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.ql.session.SessionStore;
import org.gennai.gungnir.ql.session.SessionStoreException;
import org.gennai.gungnir.tuple.persistent.PersistentDispatcher;

public interface ClusterManager {

  void start() throws ClusterManagerException;

  void join() throws ClusterManagerException;

  PersistentDispatcher getDispatcher(String accountId);

  UserEntity getUserByName(String userName);

  void sync(GungnirTopology topology) throws ClusterManagerException;

  SessionStore getSessionStore() throws SessionStoreException;

  void close();
}
