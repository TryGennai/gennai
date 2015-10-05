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
import org.gennai.gungnir.ql.FileStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DescFileTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescFileTask.class);

  private String fileName;
  private UserEntity owner;
  private ObjectMapper mapper;

  public DescFileTask(String fileName, UserEntity owner) {
    this.fileName = fileName;
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    FileStat fileStat = null;
    try {
      fileStat = GungnirManager.getManager().getMetaStore().findFile(fileName, owner);
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    ObjectNode fileNode = mapper.createObjectNode();
    fileNode.put("name", fileStat.getName());
    fileNode.put("size", fileStat.getSize());
    fileNode.put("checksum", fileStat.getChecksum());
    ArrayNode topologiesNode = mapper.createArrayNode();
    for (String topologyId : fileStat.getTopologies()) {
      topologiesNode.add(topologyId);
    }
    fileNode.set("topologies", topologiesNode);
    fileNode.put("owner", fileStat.getOwner().getName());
    fileNode.putPOJO("createTime", fileStat.getCreateTime());
    if (fileStat.getComment() != null) {
      fileNode.put("comment", fileStat.getComment());
    }

    try {
      return mapper.writeValueAsString(fileNode);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
