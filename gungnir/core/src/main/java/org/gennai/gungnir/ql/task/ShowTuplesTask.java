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
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ShowTuplesTask implements Task {

  private UserEntity owner;
  private ObjectMapper mapper;

  public ShowTuplesTask(UserEntity owner) {
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    List<Schema> schemas = null;
    try {
      schemas = GungnirManager.getManager().getMetaStore().findSchemas(owner);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    }

    ArrayNode schemasNode = mapper.createArrayNode();
    for (Schema schema : schemas) {
      if (schema instanceof TupleSchema) {
        TupleSchema tupleSchema = (TupleSchema) schema;
        ObjectNode schemaNode = mapper.createObjectNode();
        schemaNode.put("name", tupleSchema.getSchemaName());
        ArrayNode topologiesNode = mapper.createArrayNode();
        for (String topologyId : tupleSchema.getTopologies()) {
          topologiesNode.add(topologyId);
        }
        schemaNode.set("topologies", topologiesNode);
        schemaNode.put("owner", tupleSchema.getOwner().getName());
        schemaNode.putPOJO("createTime", tupleSchema.getCreateTime());
        if (tupleSchema.getComment() != null) {
          schemaNode.put("comment", tupleSchema.getComment());
        }

        schemasNode.add(schemaNode);
      }
    }

    try {
      return mapper.writeValueAsString(schemasNode);
    } catch (Exception e) {
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
