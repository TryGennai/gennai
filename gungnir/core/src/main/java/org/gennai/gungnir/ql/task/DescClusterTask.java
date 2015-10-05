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

import org.apache.thrift7.TException;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DescClusterTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescClusterTask.class);

  private UserEntity owner;
  private ObjectMapper mapper;

  public DescClusterTask(UserEntity owner) {
    this.owner = owner;
    mapper = new ObjectMapper();
  }

  @Override
  public String execute() throws TaskExecuteException {
    if (owner.getName().equals(ROOT_USER_NAME)) {
      StormClusterManager stormClusterManager = StormClusterManager.getManager();
      ObjectNode clusterNode = mapper.createObjectNode();

      try {
        if (stormClusterManager.isLocalCluster()) {
          clusterNode.put("mode", "local");
        } else {
          clusterNode.put("mode", "distributed");
        }

        ClusterSummary clusterSummary = stormClusterManager.getClusterInfo();

        ObjectNode nimbusNode = mapper.createObjectNode();
        nimbusNode.put("uptimeSecs", clusterSummary.get_nimbus_uptime_secs());
        clusterNode.set("nimbus", nimbusNode);

        ArrayNode supervisorsNode = mapper.createArrayNode();
        for (SupervisorSummary supervisorSummary : clusterSummary.get_supervisors()) {
          ObjectNode supervisorNode = mapper.createObjectNode();
          supervisorNode.put("id", supervisorSummary.get_supervisor_id());
          supervisorNode.put("host", supervisorSummary.get_host());
          supervisorNode.put("uptimeSecs", supervisorSummary.get_uptime_secs());
          supervisorNode.put("numWorkers", supervisorSummary.get_num_workers());
          supervisorNode.put("numUsedWorkers", supervisorSummary.get_num_used_workers());
          supervisorsNode.add(supervisorNode);
        }
        clusterNode.set("supervisors", supervisorsNode);

        ArrayNode topologiesNode = mapper.createArrayNode();
        for (TopologySummary topologySummary : clusterSummary.get_topologies()) {
          ObjectNode topologyNode = mapper.createObjectNode();
          topologyNode.put("name", topologySummary.get_name());
          topologyNode.put("status", topologySummary.get_status());
          topologyNode.put("uptimeSecs", topologySummary.get_uptime_secs());
          topologyNode.put("numWorkers", topologySummary.get_num_workers());
          topologyNode.put("numExecutors", topologySummary.get_num_executors());
          topologyNode.put("numTasks", topologySummary.get_num_tasks());
          topologiesNode.add(topologyNode);
        }
        clusterNode.set("topologies", topologiesNode);
      } catch (TException e) {
        throw new TaskExecuteException(e);
      }

      try {
        return mapper.writeValueAsString(clusterNode);
      } catch (Exception e) {
        LOG.error("Failed to convert json format", e);
        throw new TaskExecuteException("Failed to convert json format", e);
      }
    } else {
      LOG.warn("Permission denied {}", owner.getName());
      throw new TaskExecuteException("Permission denied");
    }
  }
}
