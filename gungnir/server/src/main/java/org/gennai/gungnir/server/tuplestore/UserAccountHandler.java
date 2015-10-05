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

package org.gennai.gungnir.server.tuplestore;

import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.cluster.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserAccountHandler implements VariableHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UserAccountHandler.class);

  private static final Pattern ACCOUNT_PATTERN = Pattern
      .compile("^ACCOUNT:(?:\\$(\\d+)|(\\w+))$", Pattern.CASE_INSENSITIVE);

  private ClusterManager clusterManager;
  private String userName;
  private Integer group;

  public UserAccountHandler() {
  }

  private UserAccountHandler(String userName) {
    this.userName = userName;
    clusterManager = GungnirManager.getManager().getClusterManager();
  }

  private UserAccountHandler(int group) {
    this.group = group;
    clusterManager = GungnirManager.getManager().getClusterManager();
  }

  @Override
  public boolean isMatch(String variable) {
    return ACCOUNT_PATTERN.matcher(variable).find();
  }

  @Override
  public UserAccountHandler build(String variable) {
    Matcher matcher = ACCOUNT_PATTERN.matcher(variable);
    if (matcher.find()) {
      if (matcher.group(1) != null) {
        return new UserAccountHandler(Integer.parseInt(matcher.group(1)));
      } else {
        return new UserAccountHandler(matcher.group(2));
      }
    }
    return null;
  }

  @Override
  public String getValue(MatchResult matchResult) {
    String name;
    if (userName != null) {
      name = userName;
    } else {
      name = matchResult.group(group);
    }

    UserEntity owner = clusterManager.getUserByName(name);
    if (owner != null) {
      return owner.getId();
    }

    LOG.warn("Topology isn't started by '{}'", name);
    return null;
  }
}
