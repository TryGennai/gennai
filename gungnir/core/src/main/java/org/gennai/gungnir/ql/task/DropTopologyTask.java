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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.FileStat;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.analysis.FileRegistry;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.utils.GungnirUtils;

public class DropTopologyTask implements Task {

  private String topologyName;
  private UserEntity owner;
  private FileRegistry fileRegistry;

  public DropTopologyTask(String topologyName, UserEntity owner, FileRegistry fileRegistry) {
    this.topologyName = topologyName;
    this.owner = owner;
    this.fileRegistry = fileRegistry;
  }

  @Override
  public String execute() throws TaskExecuteException {
    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();

      GungnirTopology topology = metaStore.findTopologyByName(topologyName, owner);
      if (topology.getStatus() != TopologyStatus.STOPPED) {
        throw new TaskExecuteException("Topology status is '" + topology.getStatus() + "'");
      }

      metaStore.deleteTopology(topology);

      List<Schema> schemas = topology.getUsedSchemas();
      for (Schema schema : schemas) {
        schema.setOwner(owner);
        metaStore.changeSchemaToFree(schema, topology.getId());
      }

      List<FunctionEntity> functions = topology.getUsedFunctions();
      for (FunctionEntity function : functions) {
        function.setOwner(owner);
        metaStore.changeFunctionToFree(function, topology.getId());
      }

      List<FileStat> files = fileRegistry.getFiles();
      if (files != null) {
        for (FileStat file : files) {
          file.setOwner(owner);
          metaStore.changeFileToFree(file, topology.getId());
        }
      }

      try {
        GungnirUtils.deleteDirectory(Paths.get(topology.getConfig().getString(LOCAL_DIR),
            TOPOLOGY_CACHE_DIR, topology.getId()));
      } catch (IOException e) {
        throw new TaskExecuteException("Failed to delete classpath");
      }
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    return "OK";
  }
}
