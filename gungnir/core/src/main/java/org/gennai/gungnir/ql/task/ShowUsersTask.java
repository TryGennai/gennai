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

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class ShowUsersTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(ShowUsersTask.class);

  private UserEntity owner;
  private ObjectMapper mapper;

  public ShowUsersTask(UserEntity owner) {
    this.owner = owner;
    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
  }

  @Override
  public String execute() throws TaskExecuteException {
    if (owner.getName().equals(ROOT_USER_NAME)) {
      List<UserEntity> users = null;
      try {
        users = GungnirManager.getManager().getMetaStore().findUserAccounts();
      } catch (MetaStoreException e) {
        throw new TaskExecuteException(e);
      }

      ArrayNode usersNode = mapper.createArrayNode();
      for (UserEntity user : users) {
        ObjectNode userNode = mapper.createObjectNode();
        userNode.put("id", user.getId());
        userNode.put("name", user.getName());
        userNode.putPOJO("createTime", user.getCreateTime());
        if (user.getLastModifyTime() != null) {
          userNode.putPOJO("lastModifyTime", user.getLastModifyTime());
        }
        usersNode.add(userNode);
      }

      try {
        return mapper.writeValueAsString(usersNode);
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
