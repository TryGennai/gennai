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

import org.apache.thrift7.TException;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.cluster.storm.TopologyStats;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StatsTopologyTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(StatsTopologyTask.class);

  private String topologyName;
  private UserEntity owner;
  private boolean extended;
  private ObjectMapper mapper;

  public StatsTopologyTask(String topologyName, UserEntity owner, boolean extended) {
    this.topologyName = topologyName;
    this.owner = owner;
    this.extended = extended;
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  @Override
  public String execute() throws TaskExecuteException {
    GungnirTopology topology = null;
    TopologyInfo topologyInfo = null;

    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();
      topology = metaStore.findTopologyByName(topologyName, owner);
      if (topology.getStatus() != TopologyStatus.RUNNING) {
        throw new TaskExecuteException("Topology status is '" + topology.getStatus() + "'");
      }

      StormClusterManager stormClusterManager = StormClusterManager.getManager();
      ClusterSummary clusterSummary = stormClusterManager.getClusterInfo();
      for (TopologySummary topologySummary : clusterSummary.get_topologies()) {
        if (topologySummary.get_name()
            .equals(StormClusterManager.getStormTopologyName(topology))) {
          topologyInfo = stormClusterManager.getTopologyInfo(topologySummary.get_id());
          break;
        }
      }
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      LOG.info("Can't find topology '{}'", topologyName);
      throw new TaskExecuteException(e);
    } catch (TException e) {
      throw new TaskExecuteException(e);
    } catch (NotAliveException e) {
      LOG.warn("Topology isn't alive '{}'", topology.getId(), e);
      throw new TaskExecuteException(e);
    }

    if (topologyInfo == null) {
      LOG.warn("Topology isn't alive '{}'", topology.getId());
      throw new TaskExecuteException(topologyName + " isn't alive");
    }

    try {
      TopologyStats topologyStats = TopologyStats.apply(topologyInfo, extended);
      return mapper.writeValueAsString(topologyStats);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
