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
import java.util.List;
import java.util.TimeZone;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.ql.FunctionEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ShowFunctionsTask implements Task {

  private UserEntity owner;
  private ObjectMapper mapper;

  public ShowFunctionsTask(UserEntity owner) {
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    List<FunctionEntity> functions = null;
    try {
      functions = GungnirManager.getManager().getMetaStore().findFunctions(owner);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    }

    ArrayNode functionsNode = mapper.createArrayNode();
    for (FunctionEntity function : functions) {
      ObjectNode functionNode = mapper.createObjectNode();
      functionNode.put("name", function.getName());
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

      functionsNode.add(functionNode);
    }

    try {
      return mapper.writeValueAsString(functionsNode);
    } catch (Exception e) {
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
