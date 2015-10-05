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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DescUserTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DescUserTask.class);

  private UserEntity user;
  private ObjectMapper mapper;

  public DescUserTask(UserEntity user) {
    this.user = user;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    try {
      GungnirManager.getManager().getMetaStore().findUserAccountById(user.getId());
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    }

    ObjectNode userNode = mapper.createObjectNode();
    userNode.put("id", user.getId());
    userNode.put("name", user.getName());
    userNode.putPOJO("createTime", user.getCreateTime());
    if (user.getLastModifyTime() != null) {
      userNode.putPOJO("lastModifyTime", user.getLastModifyTime());
    }

    try {
      return mapper.writeValueAsString(userNode);
    } catch (Exception e) {
      LOG.error("Failed to convert json format", e);
      throw new TaskExecuteException("Failed to convert json format", e);
    }
  }
}
