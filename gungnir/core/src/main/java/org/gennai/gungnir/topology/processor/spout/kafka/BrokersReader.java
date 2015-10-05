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
import static org.gennai.gungnir.topology.processor.KafkaSpoutProcessor2.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import kafka.utils.ZkUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.KeeperException;
import org.gennai.gungnir.GungnirConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.Broker;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.IBrokerReader;

import com.fasterxml.jackson.databind.ObjectMapper;

@Deprecated
public class BrokersReader implements IBrokerReader {

  private static final Logger LOG = LoggerFactory.getLogger(BrokersReader.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String topicName;
  private int numPartitions;
  private volatile CuratorFramework curator;
  private int retryTimes;
  private int retryInterval;

  public BrokersReader(GungnirConfig config, String topicName, int numPartitions) {
    this.topicName = topicName;
    this.numPartitions = numPartitions;

    List<String> zkServers = config.getList(KAFKA_ZOOKEEPER_SERVERS);
    retryTimes = config.getInteger(READ_BROKERS_RETRY_TIMES);
    retryInterval = config.getInteger(READ_BROKERS_RETRY_INTERVAL);

    curator = CuratorFrameworkFactory.builder()
        .connectString(StringUtils.join(zkServers, ","))
        .sessionTimeoutMs(config.getInteger(KAFKA_ZOOKEEPER_SESSION_TIMEOUT))
        .connectionTimeoutMs(config.getInteger(KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT))
        .retryPolicy(new RetryNTimes(config.getInteger(KAFKA_ZOOKEEPER_RETRY_TIMES),
            config.getInteger(KAFKA_ZOOKEEPER_RETRY_INTERVAL))).build();

    curator.start();
  }

  public int getNumPartitions() throws Exception {
    String path = ZkUtils.getTopicPartitionsPath(topicName);
    List<String> nodes = curator.getChildren().forPath(path);
    return nodes.size();
  }

  private int getLeader(int partition) throws Exception {
    String path = ZkUtils.getTopicPartitionLeaderAndIsrPath(topicName, partition);

    for (int i = 0; i < retryTimes; i++) {
      try {
        byte[] bytes = curator.getData().forPath(path);
        @SuppressWarnings("unchecked")
        Map<String, Object> info = MAPPER.readValue(bytes, HashMap.class);
        return ((Number) info.get("leader")).intValue();
      } catch (KeeperException.NoNodeException e) {
        TimeUnit.MILLISECONDS.sleep(retryInterval);
      }
    }

    return -1;
  }

  public Broker getBrokerHost(int brokerId) throws Exception {
    try {
      byte[] bytes = curator.getData().forPath(ZkUtils.BrokerIdsPath() + "/" + brokerId);
      @SuppressWarnings("unchecked")
      Map<String, Object> info = MAPPER.readValue(bytes, HashMap.class);
      String host = info.get("host").toString();
      int port = ((Number) info.get("port")).intValue();
      return new Broker(host, port);
    } catch (KeeperException.NoNodeException e) {
      LOG.error("Can't find broker host. brokerId: {}", brokerId);
      return null;
    }
  }

  @Override
  public GlobalPartitionInformation getCurrentBrokers() {
    GlobalPartitionInformation brokersInfo = new GlobalPartitionInformation();

    try {
      for (int partition = 0; partition < numPartitions; partition++) {
        int leader = getLeader(partition);
        if (leader >= 0) {
          Broker broker = getBrokerHost(leader);
          if (broker != null) {
            brokersInfo.addPartition(partition, broker);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to get current brokers", e);
    }

    return brokersInfo;
  }

  public void close() {
    if (curator != null) {
      synchronized (this) {
        if (curator != null) {
          curator.close();
          curator = null;
        }
      }
    }
  }
}
