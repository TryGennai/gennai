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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.gennai.gungnir.GungnirConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import storm.kafka.Broker;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.IBrokerReader;

import com.google.common.collect.Lists;

public class BrokersReader2 implements IBrokerReader {

  private static final Logger LOG = LoggerFactory.getLogger(BrokersReader2.class);

  private GungnirConfig config;
  private String topicName;
  private ZkClient zkClient;
  private int retryTimes;
  private int retryInterval;

  public BrokersReader2(GungnirConfig config, ZkHosts hosts, String topicName) {
    this.config = config;
    this.topicName = topicName;

    zkClient = new ZkClient(hosts.brokerZkStr,
        config.getInteger(KAFKA_ZOOKEEPER_SESSION_TIMEOUT),
        config.getInteger(KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT),
        new ZkSerializer() {
          @Override
          public byte[] serialize(Object data) {
            return ZKStringSerializer.serialize(data);
          }

          @Override
          public Object deserialize(byte[] bytes) {
            return ZKStringSerializer.deserialize(bytes);
          }
        });

    retryTimes = config.getInteger(READ_BROKERS_RETRY_TIMES);
    retryInterval = config.getInteger(READ_BROKERS_RETRY_INTERVAL);
  }

  private int getNumPartitions() {
    Map<TopicAndPartition, Seq<Object>> existingPartitionsReplicaList =
        JavaConverters.mapAsJavaMapConverter(
            ZkUtils.getReplicaAssignmentForTopics(zkClient,
                JavaConverters.asScalaBufferConverter(Lists.newArrayList(topicName)).asScala()
                    .toSeq())).asJava();
    return existingPartitionsReplicaList.size();
  }

  public void createTopic(int numPartitions) {
    if (!AdminUtils.topicExists(zkClient, topicName)) {
      try {
        Integer replFactor = config.getInteger(REPLICATION_FACTOR);
        AdminUtils.createTopic(zkClient, topicName, numPartitions, replFactor, new Properties());
        LOG.info("Topic created. name: {}, partitions: {}, replFactor: {}", topicName,
            numPartitions, replFactor);
      } catch (TopicExistsException ignore) {
        LOG.info("Topic exists. name: {}", topicName);
      }
    } else {
      LOG.info("Topic exists. name: {}", topicName);
      if (numPartitions > getNumPartitions()) {
        try {
          AdminUtils.addPartitions(zkClient, topicName, numPartitions, "", true, new Properties());
          LOG.info("Topic altered. name: {}, partitions: {}", topicName, numPartitions);
        } catch (AdminOperationException e) {
          LOG.error("Failed to add partitions", e);
        }
      }
    }
  }

  private int getLeader(int partition) {
    try {
      for (int i = 0; i < retryTimes; i++) {
        Option<Object> option = ZkUtils.getLeaderForPartition(zkClient, topicName, partition);
        if (!option.isEmpty()) {
          return ((Number) option.get()).intValue();
        }
        TimeUnit.MILLISECONDS.sleep(retryInterval);
      }
    } catch (InterruptedException ignore) {
      ignore = null;
    }
    LOG.error("Failed to get leader for partition. topic: {} partition: {}", topicName, partition);
    return -1;
  }

  private Broker getBrokerHost(int brokerId) {
    try {
      for (int i = 0; i < retryTimes; i++) {
        Option<kafka.cluster.Broker> option = ZkUtils.getBrokerInfo(zkClient, brokerId);
        if (!option.isEmpty()) {
          kafka.cluster.Broker broker = (kafka.cluster.Broker) option.get();
          return new Broker(broker.host(), broker.port());
        }
        TimeUnit.MILLISECONDS.sleep(retryInterval);
      }
    } catch (InterruptedException ignore) {
      ignore = null;
    }
    LOG.error("Failed to get broker info. brokerId: {}", brokerId);
    return null;
  }

  @Override
  public GlobalPartitionInformation getCurrentBrokers() {
    GlobalPartitionInformation brokersInfo = new GlobalPartitionInformation();

    try {
      int numPartitions = getNumPartitions();
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
    if (zkClient != null) {
      zkClient.close();
    }
  }
}
