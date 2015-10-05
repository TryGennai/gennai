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
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.cluster.ClusterManager;
import org.gennai.gungnir.cluster.ClusterManagerException;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.cluster.storm.StormClusterManagerException;
import org.gennai.gungnir.cluster.storm.TopologyStatusChangedListener;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.NotAliveException;

public class StopTopologyTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(StopTopologyTask.class);

  private String topologyName;
  private UserEntity owner;
  private GungnirTopology topology;
  private GungnirManager manager;
  private MetaStore metaStore;
  private ClusterManager clusterManager;

  public StopTopologyTask(String topologyName, UserEntity owner) {
    this.topologyName = topologyName;
    this.owner = owner;
  }

  private void rollback() throws MetaStoreException {
    topology.setStatus(TopologyStatus.RUNNING);
    manager.getMetaStore().changeForcedTopologyStatus(topology);

    try {
      clusterManager.sync(topology);
    } catch (Exception e) {
      LOG.error("Failed to synchronize the cluster", e);
    }

    LOG.error("Topology rollback '{}'", topology.getId());
  }

  private class TopologyStoppedListener implements TopologyStatusChangedListener {

    @Override
    public void process() {
      try {
        topology.setStatus(TopologyStatus.STOPPED);
        if (!metaStore.changeTopologyStatus(topology)) {
          TopologyStatus status = metaStore.getTopologyStatus(topology.getId());
          LOG.error("Can't change topology status '{}' ({}->{})", topology.getId(), status,
              TopologyStatus.RUNNING);
          rollback();
        }
      } catch (MetaStoreException e) {
        LOG.error("Failed to change topology status", e);
      } catch (NotStoredException e) {
        LOG.error("Failed to change topology status", e);
      }
    }

    @Override
    public void rollback() {
      try {
        StopTopologyTask.this.rollback();
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
      if (topology.getStatus() != TopologyStatus.RUNNING) {
        throw new TaskExecuteException("Topology status is '" + topology.getStatus() + "'");
      }

      topology.setStatus(TopologyStatus.STOPPING);
      if (!metaStore.changeTopologyStatus(topology)) {
        TopologyStatus status = metaStore.getTopologyStatus(topology.getId());
        throw new TaskExecuteException("Topology status is '" + status + "'");
      }

      clusterManager = manager.getClusterManager();
      clusterManager.sync(topology);

      StormClusterManager.getManager().stopTopology(topology, new TopologyStoppedListener());
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      LOG.info("Can't find topology '{}'", topologyName);
      throw new TaskExecuteException(e);
    } catch (NotAliveException e) {
      LOG.warn("Topology isn't alive '{}'", topology.getId(), e);
    } catch (StormClusterManagerException e) {
      try {
        rollback();
      } catch (MetaStoreException e2) {
        throw new TaskExecuteException(e2);
      }
      throw new TaskExecuteException(e);
    } catch (ClusterManagerException e) {
      LOG.error("Failed to synchronize the cluster", e);
      throw new TaskExecuteException(e);
    }

    return "OK";
  }
}
