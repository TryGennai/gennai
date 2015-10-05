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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.cluster.ClusterManagerException;
import org.gennai.gungnir.cluster.storm.CapacityWorkerException;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.cluster.storm.StormClusterManagerException;
import org.gennai.gungnir.cluster.storm.TopologyStatusChangedListener;
import org.gennai.gungnir.metastore.AlreadyStoredException;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.FileStat;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.analysis.FileRegistry;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.NotAliveException;

public class SubmitTopologyTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(SubmitTopologyTask.class);

  private GungnirTopology topology;
  private FileRegistry fileRegistry;
  private GungnirManager manager;
  private MetaStore metaStore;
  private List<Schema> schemas;
  private List<FunctionEntity> functions;
  private List<FileStat> files;
  private int schemaCnt = 0;
  private int funcCnt = 0;
  private int fileCnt = 0;
  private boolean started;

  public SubmitTopologyTask(GungnirTopology topology, FileRegistry fileRegistry) {
    this.topology = topology;
    this.fileRegistry = fileRegistry;
  }

  private void rollback() throws MetaStoreException {
    if (started) {
      try {
        if (!StormClusterManager.getManager().stopTopologySync(topology)) {
          LOG.error("Failed to stop topology '{}'", topology.getId());
        }
      } catch (NotAliveException e) {
        LOG.info("Topology isn't alive '" + topology.getId() + "'", e);
      } catch (StormClusterManagerException e) {
        LOG.error("Failed to stop topology '{}'", topology.getId(), e);
      } catch (InterruptedException e) {
        LOG.error("Failed to stop topology '{}'", topology.getId(), e);
      }
    }

    for (int i = 0; i < fileCnt; i++) {
      metaStore.changeFileToFree(files.get(i), topology.getId());
    }

    for (int i = 0; i < funcCnt; i++) {
      metaStore.changeFunctionToFree(functions.get(i), topology.getId());
    }

    for (int i = 0; i < schemaCnt; i++) {
      metaStore.changeSchemaToFree(schemas.get(i), topology.getId());
    }

    try {
      metaStore.deleteTopology(topology);
    } catch (NotStoredException e) {
      LOG.warn("Failed to delete topology", e);
    }

    LOG.error("Topology rollback '{}'", topology.getId());
  }

  private class TopologyStartedListener implements TopologyStatusChangedListener {

    @Override
    public void process() {
      try {
        topology.setStatus(TopologyStatus.RUNNING);

        if (metaStore.changeTopologyStatus(topology)) {
          manager.getClusterManager().sync(topology);
        } else {
          TopologyStatus status = metaStore.getTopologyStatus(topology.getId());
          LOG.error("Can't change topology status '{}' ({}->{})", topology.getId(), status,
              TopologyStatus.RUNNING);
          rollback();
        }
      } catch (MetaStoreException e) {
        LOG.error("Failed to change topology status", e);
      } catch (NotStoredException e) {
        LOG.error("Failed to change topology status", e);
      } catch (ClusterManagerException e) {
        LOG.error("Failed to synchronize the cluster", e);
      }
    }

    @Override
    public void rollback() {
      try {
        SubmitTopologyTask.this.rollback();
      } catch (MetaStoreException e) {
        LOG.error("Failed to rollback status", e);
      }
    }
  }

  @Override
  public String execute() throws TaskExecuteException {
    if (topology.getStatus() != TopologyStatus.STOPPED) {
      LOG.info("Topology status is '{}'", topology.getStatus());
      throw new TaskExecuteException("Topology status is '" + topology.getStatus() + "'");
    }

    manager = GungnirManager.getManager();

    try {
      metaStore = manager.getMetaStore();

      topology.setStatus(TopologyStatus.STARTING);
      metaStore.insertTopology(topology);

      schemas = topology.getUsedSchemas();
      for (; schemaCnt < schemas.size(); schemaCnt++) {
        Schema schema = schemas.get(schemaCnt);
        schema.setOwner(topology.getOwner());
        if (!metaStore.changeSchemaToBusy(schema, topology.getId())) {
          rollback();
          throw new TaskExecuteException(schema.getSchemaName() + " can't change to busy");
        }
      }

      functions = topology.getUsedFunctions();
      for (; funcCnt < functions.size(); funcCnt++) {
        FunctionEntity function = functions.get(funcCnt);
        function.setOwner(topology.getOwner());
        if (!metaStore.changeFunctionToBusy(function, topology.getId())) {
          rollback();
          throw new TaskExecuteException(function.getName() + " can't change to busy");
        }
      }

      files = fileRegistry.getFiles();
      if (files != null) {
        for (; fileCnt < files.size(); fileCnt++) {
          FileStat fileStat = files.get(fileCnt);
          if (!metaStore.changeFileToBusy(fileStat, topology.getId())) {
            rollback();
            throw new TaskExecuteException("'" + fileStat.getName() + "' can't change to busy");
          }
        }
      }

      Path classPath = Paths.get(topology.getConfig().getString(LOCAL_DIR), TOPOLOGY_CACHE_DIR,
          topology.getId());
      try {
        GungnirUtils.createLinkDirectory(classPath, Paths.get(fileRegistry.getCacheDir()));
      } catch (IOException e) {
        rollback();
        throw new TaskExecuteException("Failed to link classpath");
      }

      StormClusterManager.getManager().startTopology(topology, new TopologyStartedListener());
      started = true;
    } catch (MetaStoreException e) {
      topology.setStatus(TopologyStatus.STOPPED);
      throw new TaskExecuteException(e);
    } catch (AlreadyStoredException e) {
      topology.setStatus(TopologyStatus.STOPPED);
      throw new TaskExecuteException(e);
    } catch (StormClusterManagerException e) {
      try {
        rollback();
      } catch (MetaStoreException e2) {
        throw new TaskExecuteException(e2);
      }
      throw new TaskExecuteException(e);
    } catch (CapacityWorkerException e) {
      try {
        rollback();
      } catch (MetaStoreException e2) {
        throw new TaskExecuteException(e2);
      }
      throw new TaskExecuteException(e);
    }

    return "OK";
  }
}
