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

package org.gennai.gungnir.ql.task;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.session.SessionStoreException;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropUserTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DropUserTask.class);

  private String userName;
  private UserEntity owner;

  public DropUserTask(String userName, UserEntity owner) {
    this.userName = userName;
    this.owner = owner;
  }

  @Override
  public String execute() throws TaskExecuteException {
    if (owner.getName().equals(ROOT_USER_NAME) && !userName.equals(ROOT_USER_NAME)) {
      try {
        GungnirManager manager = GungnirManager.getManager();
        MetaStore metaStore = manager.getMetaStore();

        UserEntity user = metaStore.findUserAccountByName(userName);

        manager.getClusterManager().getSessionStore().deleteAllSessions(user.getId());

        List<GungnirTopology> topologies = metaStore.findTopologies(user);
        if (!topologies.isEmpty()) {
          throw new TaskExecuteException("Can't delete because topology exists");
        }

        List<Schema> schemas = metaStore.findSchemas(user);
        if (!schemas.isEmpty()) {
          throw new TaskExecuteException("Can't delete because schema exists");
        }

        metaStore.deleteUserAccount(user);
      } catch (MetaStoreException e) {
        throw new TaskExecuteException(e);
      } catch (NotStoredException e) {
        throw new TaskExecuteException(e);
      } catch (SessionStoreException e) {
        throw new TaskExecuteException(e);
      }
    } else {
      LOG.warn("Permission denied {}", owner.getName());
      throw new TaskExecuteException("Permission denied");
    }

    return "OK";
  }
}
