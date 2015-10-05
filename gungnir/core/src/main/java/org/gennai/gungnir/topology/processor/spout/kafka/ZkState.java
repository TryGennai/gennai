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

package org.gennai.gungnir.topology.processor.spout.kafka;

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.gennai.gungnir.GungnirConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ZkState {

  private static final Logger LOG = LoggerFactory.getLogger(ZkState.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CuratorFramework curator;

  public ZkState(GungnirConfig config) {
    List<String> zkServers = config.getList(KAFKA_ZOOKEEPER_SERVERS);

    curator = CuratorFrameworkFactory.builder()
        .connectString(StringUtils.join(zkServers, ","))
        .sessionTimeoutMs(config.getInteger(KAFKA_ZOOKEEPER_SESSION_TIMEOUT))
        .connectionTimeoutMs(config.getInteger(KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT))
        .retryPolicy(new RetryNTimes(config.getInteger(KAFKA_ZOOKEEPER_RETRY_TIMES),
            config.getInteger(KAFKA_ZOOKEEPER_RETRY_INTERVAL))).build();

    curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {

      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        LOG.info("Connection state changed. state: {}", newState);
      }
    });

    curator.start();
  }

  public void writeJSON(String path, Map<Object, Object> state) throws Exception {
    byte[] bytes = MAPPER.writeValueAsBytes(state);
    if (curator.checkExists().forPath(path) == null) {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .forPath(path, bytes);
    } else {
      curator.setData().forPath(path, bytes);
    }
  }

  public Map<String, Object> readJSON(String path) throws Exception {
    if (curator.checkExists().forPath(path) != null) {
      byte[] bytes = curator.getData().forPath(path);
      Map<String, Object> state = MAPPER.readValue(bytes,
          MAPPER.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class));
      return state;
    } else {
      return null;
    }
  }

  public void close() {
    curator.close();
  }
}
