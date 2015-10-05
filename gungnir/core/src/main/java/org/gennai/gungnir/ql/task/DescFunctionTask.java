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

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.FunctionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DescFunctionTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescFunctionTask.class);

  private String functionName;
  private UserEntity owner;
  private ObjectMapper mapper;

  public DescFunctionTask(String functionName, UserEntity owner) {
    this.functionName = functionName;
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    FunctionEntity function = null;
    try {
      function = GungnirManager.getManager().getMetaStore().findFunction(functionName, owner);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    ObjectNode functionNode = mapper.createObjectNode();
    functionNode.put("name", function.getName());
    functionNode.put("type", function.getType().toString());
    functionNode.put("location", function.getLocation());
    ArrayNode topologiesNode = mapper.createArrayNode();
    for (String topologyId : function.getTopologies()) {
      topologiesNode.add(topologyId);
    }
    functionNode.set("topologies", topologiesNode);
    functionNode.put("owner", function.getOwner().getName());
    functionNode.putPOJO("createTime", function.getCreateTime());
    if (function.getComment() != null) {
      functionNode.put("comment", function.getComment());
    }

    try {
      return mapper.writeValueAsString(functionNode);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
