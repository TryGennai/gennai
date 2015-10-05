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
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.ViewSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class DescViewTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescViewTask.class);

  private String schemaName;
  private UserEntity owner;
  private ObjectMapper mapper;

  public DescViewTask(String schemaName, UserEntity owner) {
    this.schemaName = schemaName;
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    Schema schema = null;
    try {
      schema = GungnirManager.getManager().getMetaStore().findSchema(schemaName, owner);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    ViewSchema viewSchema = null;
    if (schema instanceof ViewSchema) {
      viewSchema = (ViewSchema) schema;
    } else {
      throw new TaskExecuteException(schemaName + " isn't view");
    }

    ObjectNode schemaNode = mapper.createObjectNode();
    schemaNode.put("name", viewSchema.getSchemaName());
    schemaNode.put("from", viewSchema.getTupleSchema().getSchemaName());
    schemaNode.put("filter", viewSchema.getCondition().toString());
    ArrayNode topologiesNode = mapper.createArrayNode();
    for (String topologyId : viewSchema.getTopologies()) {
      topologiesNode.add(topologyId);
    }
    schemaNode.set("topologies", topologiesNode);
    schemaNode.put("owner", viewSchema.getOwner().getName());
    schemaNode.putPOJO("createTime", viewSchema.getCreateTime());
    if (viewSchema.getComment() != null) {
      schemaNode.put("comment", viewSchema.getComment());
    }

    try {
      return mapper.writeValueAsString(schemaNode);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
