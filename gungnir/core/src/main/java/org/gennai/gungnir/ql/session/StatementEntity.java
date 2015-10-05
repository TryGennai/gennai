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
import java.util.Map;
import java.util.Set;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.ql.stream.Stream;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StatementEntity implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String statementId;
  private String sessionId;
  private UserEntity owner;
  private GungnirConfig config;
  private GungnirTopology topology;
  private Map<String, Stream> streamsMap;
  private Set<String> streamTuples;
  private Map<String, String> aliasNamesMap;
  private String uploadingFileName;

  public StatementEntity(String statementId, String sessionId, UserEntity owner) {
    this.statementId = statementId;
    this.sessionId = sessionId;
    this.owner = owner;
    config = GungnirManager.getManager().getConfig().clone();
  }

  public String getStatementId() {
    return statementId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setOwner(UserEntity owner) {
    this.owner = owner;
  }

  public GungnirConfig getConfig() {
    return config;
  }

  public UserEntity getOwner() {
    return owner;
  }

  public void setTopology(GungnirTopology topology) {
    this.topology = topology;
  }

  public GungnirTopology getTopology() {
    return topology;
  }

  public void setStreamsMap(Map<String, Stream> streamsMap) {
    this.streamsMap = streamsMap;
  }

  public Map<String, Stream> getStreamsMap() {
    if (streamsMap == null) {
      streamsMap = Maps.newHashMap();
    }
    return streamsMap;
  }

  public void setStreamTuples(Set<String> streamTuples) {
    this.streamTuples = streamTuples;
  }

  public Set<String> getStreamTuples() {
    if (streamTuples == null) {
      streamTuples = Sets.newHashSet();
    }
    return streamTuples;
  }

  public void setAliasNamesMap(Map<String, String> aliasNamesMap) {
    this.aliasNamesMap = aliasNamesMap;
  }

  public Map<String, String> getAliasNamesMap() {
    if (aliasNamesMap == null) {
      aliasNamesMap = Maps.newHashMap();
    }
    return aliasNamesMap;
  }

  public void setUploadingFileName(String uploadingFileName) {
    this.uploadingFileName = uploadingFileName;
  }

  public String getUploadingFileName() {
    return uploadingFileName;
  }

  public void clear() {
    topology = null;
    streamsMap = null;
    streamTuples = null;
    aliasNamesMap = null;
    uploadingFileName = null;
  }
}
