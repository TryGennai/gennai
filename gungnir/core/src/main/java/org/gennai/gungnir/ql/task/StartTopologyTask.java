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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.cluster.ClusterManagerException;
import org.gennai.gungnir.cluster.storm.CapacityWorkerException;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.cluster.storm.StormClusterManagerException;
import org.gennai.gungnir.cluster.storm.TopologyStatusChangedListener;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.analysis.FileRegistry;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.NotAliveException;

public class StartTopologyTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(StartTopologyTask.class);

  private String topologyName;
  private UserEntity owner;
  private FileRegistry fileRegistry;
  private GungnirTopology topology;
  private GungnirManager manager;
  private MetaStore metaStore;
  private boolean started;

  public StartTopologyTask(String topologyName, UserEntity owner, FileRegistry fileRegistry) {
    this.topologyName = topologyName;
    this.owner = owner;
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

    topology.setStatus(TopologyStatus.STOPPED);
    manager.getMetaStore().changeForcedTopologyStatus(topology);

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
        StartTopologyTask.this.rollback();
      } catch (MetaStoreException e) {
        LOG.error("Failed to rollback status", e);
      }
    }
  }

  @Override
  public String execute() throws TaskExecuteException {
    manager = GungnirManager.getManager();

    try {
      metaStore = manager.getMetaStore();

      topology = metaStore.findTopologyByName(topologyName, owner);
      if (topology.getStatus() != TopologyStatus.STOPPED) {
        LOG.info("Topology status is '{}'", topology.getStatus());
        throw new TaskExecuteException("Topology status is '" + topology.getStatus() + "'");
      }

      topology.setStatus(TopologyStatus.STARTING);
      if (!metaStore.changeTopologyStatus(topology)) {
        TopologyStatus status = metaStore.getTopologyStatus(topology.getId());
        LOG.info("Topology status is '{}'", status);
        throw new TaskExecuteException("Topology status is '" + status + "'");
      }

      Path classPath = Paths.get(topology.getConfig().getString(LOCAL_DIR), TOPOLOGY_CACHE_DIR,
          topology.getId());
      if (!Files.exists(classPath)) {
        try {
          GungnirUtils.createLinkDirectory(classPath, Paths.get(fileRegistry.getCacheDir()));
        } catch (IOException e) {
          rollback();
          throw new TaskExecuteException("Failed to link classpath");
        }
      }

      StormClusterManager.getManager().startTopology(topology, new TopologyStartedListener());
      started = true;
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      LOG.info("Can't find topology '{}'", topologyName);
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
