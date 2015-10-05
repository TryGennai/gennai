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

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterUserTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(AlterUserTask.class);

  private UserEntity user;
  private UserEntity owner;

  public AlterUserTask(UserEntity user, UserEntity owner) {
    this.user = user;
    this.owner = owner;
  }

  @Override
  public String execute() throws TaskExecuteException {
    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();
      if (owner.getName().equals(ROOT_USER_NAME)) {
        byte[] password = user.getPassword();
        user = metaStore.findUserAccountByName(user.getName());
        user.setPassword(password);
        metaStore.changeUserAccountPassword(user);
      } else {
        if (user.getName().equals(owner.getName())) {
          owner.setPassword(user.getPassword());
          metaStore.changeUserAccountPassword(owner);
        } else {
          LOG.warn("Permission denied {}", owner.getName());
          throw new TaskExecuteException("Permission denied");
        }
      }

      return "OK";
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }
  }
}
