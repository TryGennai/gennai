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

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.tuple.schema.Schema;

public class DropSchemaTask implements Task {

  private String schemaName;
  private UserEntity owner;

  public DropSchemaTask(String schemaName, UserEntity owner) {
    this.schemaName = schemaName;
    this.owner = owner;
  }

  @Override
  public String execute() throws TaskExecuteException {
    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();

      Schema schema = metaStore.findSchema(schemaName, owner);
      if (!schema.getTopologies().isEmpty()) {
        throw new TaskExecuteException(schemaName + " has been used");
      }

      if (!metaStore.deleteSchema(schema)) {
        throw new TaskExecuteException(schemaName + " has been used");
      }
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    return "OK";
  }
}
