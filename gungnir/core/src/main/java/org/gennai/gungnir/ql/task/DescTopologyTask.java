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

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.thrift7.TException;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DescTopologyTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescTopologyTask.class);

  private String topologyName;
  private UserEntity owner;
  private ObjectMapper mapper;

  public DescTopologyTask(String topologyName, UserEntity owner) {
    this.topologyName = topologyName;
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    ObjectNode topologyNode = mapper.createObjectNode();

    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();
      GungnirTopology topology = metaStore.findTopologyByName(topologyName, owner);

      String status = null;
      ObjectNode summaryNode = null;
      if (topology.getStatus() == TopologyStatus.RUNNING) {
        ClusterSummary clusterSummary = StormClusterManager.getManager().getClusterInfo();
        for (TopologySummary topologySummary : clusterSummary.get_topologies()) {
          if (topologySummary.get_name()
              .equals(StormClusterManager.getStormTopologyName(topology))) {
            summaryNode = mapper.createObjectNode();
            summaryNode.put("name", topologySummary.get_name());
            summaryNode.put("status", topologySummary.get_status());
            summaryNode.put("uptimeSecs", topologySummary.get_uptime_secs());
            summaryNode.put("numWorkers", topologySummary.get_num_workers());
            summaryNode.put("numExecutors", topologySummary.get_num_executors());
            summaryNode.put("numTasks", topologySummary.get_num_tasks());

            if ("ACTIVE".equals(topologySummary.get_status())) {
              status = topology.getStatus().toString();
            }
            break;
          }
        }
        if (status == null) {
          status = "WARN:INACTIVE";
        }
      } else {
        status = topology.getStatus().toString();
      }

      topologyNode.put("id", topology.getId());
      topologyNode.put("name", topology.getName());
      topologyNode.put("status", status);
      topologyNode.put("owner", topology.getOwner().getName());
      topologyNode.putPOJO("createTime", topology.getCreateTime());
      if (topology.getComment() != null) {
        topologyNode.put("comment", topology.getComment());
      }

      if (summaryNode != null) {
        topologyNode.set("summary", summaryNode);
      }
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    } catch (TException e) {
      throw new TaskExecuteException(e);
    }

    try {
      return mapper.writeValueAsString(topologyNode);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
