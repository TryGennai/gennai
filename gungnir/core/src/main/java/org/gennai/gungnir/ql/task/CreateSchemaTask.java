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
import org.gennai.gungnir.metastore.AlreadyStoredException;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.SchemaValidateException;

public class CreateSchemaTask implements Task {

  private Schema schema;

  public CreateSchemaTask(Schema schema) {
    this.schema = schema;
  }

  @Override
  public String execute() throws TaskExecuteException {
    try {
      schema.validate();
      GungnirManager.getManager().getMetaStore().insertSchema(schema);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (AlreadyStoredException e) {
      throw new TaskExecuteException(e);
    } catch (SchemaValidateException e) {
      throw new TaskExecuteException(e);
    }
    return "OK";
  }
}
