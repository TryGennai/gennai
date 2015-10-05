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

package org.gennai.gungnir.ql.session;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;

import org.gennai.gungnir.utils.GungnirUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SessionEntity implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String sessionId;
  private String accountId;
  private int timeoutSecs;
  private int expire;

  public SessionEntity(String sessionId, String accountId, int timeoutSecs) {
    this.sessionId = sessionId;
    this.accountId = accountId;
    this.timeoutSecs = timeoutSecs;
    expire = GungnirUtils.currentTimeSecs() + timeoutSecs;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getAccountId() {
    return accountId;
  }

  @JsonIgnore
  public boolean isExpired() {
    if (GungnirUtils.currentTimeSecs() > expire) {
      return true;
    }
    expire = GungnirUtils.currentTimeSecs() + timeoutSecs;
    return false;
  }

  public int getExpire() {
    return expire;
  }
}
