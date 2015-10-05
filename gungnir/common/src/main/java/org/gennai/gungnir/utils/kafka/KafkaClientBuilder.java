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

package org.gennai.gungnir.utils.kafka;

import java.util.List;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;

import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.commons.lang.StringUtils;

public final class KafkaClientBuilder {

  public enum OffsetRequest {
    SMALLEST("smallest"), LARGEST("largest");

    private String timeString;

    private OffsetRequest(String timeString) {
      this.timeString = timeString;
    }
  }

  public enum ProducerType {
    SYNC, ASYNC
  }

  public interface BrokersDeclarer {

    ProducerBuildDeclarer brokers(List<String> brokers);
  }

  public interface ProducerBuildDeclarer {

    ProducerBuildDeclarer requiredAcks(int requiredAcks);

    ProducerBuildDeclarer keySerializer(Class<? extends Encoder<?>> keySerClass);

    ProducerBuildDeclarer partitioner(Class<? extends Partitioner> partitionerClass);

    ProducerBuildDeclarer producerType(ProducerType producerType);

    <K> Producer<K, byte[]> build();
  }

  public static class KafkaProducerBuilder implements BrokersDeclarer, ProducerBuildDeclarer {

    private List<String> brokers;
    private int requiredAcks;
    private Class<? extends Encoder<?>> keySerClass;
    private Class<? extends Partitioner> partitionerClass;
    private ProducerType producerType;

    @Override
    public ProducerBuildDeclarer brokers(List<String> brokers) {
      this.brokers = brokers;
      return this;
    }

    @Override
    public ProducerBuildDeclarer requiredAcks(int requiredAcks) {
      this.requiredAcks = requiredAcks;
      return this;
    }

    @Override
    public ProducerBuildDeclarer keySerializer(Class<? extends Encoder<?>> keySerClass) {
      this.keySerClass = keySerClass;
      return this;
    }

    @Override
    public ProducerBuildDeclarer partitioner(Class<? extends Partitioner> partitionerClass) {
      this.partitionerClass = partitionerClass;
      return this;
    }

    @Override
    public ProducerBuildDeclarer producerType(ProducerType producerType) {
      this.producerType = producerType;
      return this;
    }

    @Override
    public <K> Producer<K, byte[]> build() {
      Properties props = new Properties();
      props.put("metadata.broker.list", StringUtils.join(brokers, ","));
      if (keySerClass != null) {
        props.put("key.serializer.class", keySerClass.getName());
      }
      if (partitionerClass != null) {
        props.put("partitioner.class", partitionerClass.getName());
      }
      props.put("request.required.acks", String.valueOf(requiredAcks));
      if (producerType != null) {
        props.put("producer.type", producerType.toString().toLowerCase());
      }

      ProducerConfig producerConfig = new ProducerConfig(props);
      return new Producer<K, byte[]>(producerConfig);
    }
  }

  public interface GroupIdDeclarer {

    OffsetRequestDeclarer groupId(String groupId);
  }

  public interface OffsetRequestDeclarer {

    ZkServersDeclarer offset(OffsetRequest offsetRequest);
  }

  public interface ZkServersDeclarer {

    OptionalDeclarer zkServers(List<String> zkServers);
  }

  public interface ConsumerBuildDeclarer {

    ConsumerConnector build() throws InvalidConfigException;
  }

  public interface OptionalDeclarer extends ConsumerBuildDeclarer {

    OptionalDeclarer autoCommitInterval(Integer autoCommitInterval);

    OptionalDeclarer zkSessionTimeout(Integer zkSessionTimeout);

    OptionalDeclarer zkConnectionTimeout(Integer zkConnectionTimeout);

    OptionalDeclarer zkSyncTimeout(Integer zkSyncTimeout);
  }

  public static class KafkaConsumerBuilder implements GroupIdDeclarer, OffsetRequestDeclarer,
      ZkServersDeclarer, OptionalDeclarer {

    private String groupId;
    private OffsetRequest offsetRequest;
    private List<String> zkServers;
    private Integer autoCommitInterval;
    private Integer zkSessionTimeout;
    private Integer zkConnectionTimeout;
    private Integer zkSyncTimeout;

    @Override
    public OffsetRequestDeclarer groupId(String groupId) {
      this.groupId = groupId;
      return this;
    }

    @Override
    public ZkServersDeclarer offset(OffsetRequest offsetRequest) {
      this.offsetRequest = offsetRequest;
      return this;
    }

    @Override
    public OptionalDeclarer zkServers(List<String> zkServers) {
      this.zkServers = zkServers;
      return this;
    }

    @Override
    public OptionalDeclarer autoCommitInterval(Integer autoCommitInterval) {
      this.autoCommitInterval = autoCommitInterval;
      return this;
    }

    @Override
    public OptionalDeclarer zkSessionTimeout(Integer zkSessionTimeout) {
      this.zkSessionTimeout = zkSessionTimeout;
      return this;
    }

    @Override
    public OptionalDeclarer zkConnectionTimeout(Integer zkConnectionTimeout) {
      this.zkConnectionTimeout = zkConnectionTimeout;
      return this;
    }

    @Override
    public OptionalDeclarer zkSyncTimeout(Integer zkSyncTimeout) {
      this.zkSyncTimeout = zkSyncTimeout;
      return this;
    }

    @Override
    public ConsumerConnector build() throws InvalidConfigException {
      Properties props = new Properties();
      props.put("group.id", groupId);
      props.put("auto.offset.reset", offsetRequest.timeString);
      props.put("zookeeper.connect", StringUtils.join(zkServers, ","));
      if (autoCommitInterval != null) {
        props.put("auto.commit.interval.ms", String.valueOf(autoCommitInterval));
      }
      if (zkConnectionTimeout != null) {
        props.put("zookeeper.connection.timeout.ms", String.valueOf(zkConnectionTimeout));
      }
      if (zkSessionTimeout != null) {
        props.put("zookeeper.session.timeout.ms", String.valueOf(zkSessionTimeout));
      }
      if (zkSyncTimeout != null) {
        props.put("zookeeper.sync.time.ms", String.valueOf(zkSyncTimeout));
      }

      ConsumerConfig consumerConfig = new ConsumerConfig(props);
      try {
        return Consumer.createJavaConsumerConnector(consumerConfig);
      } catch (ZkTimeoutException e) {
        throw new InvalidConfigException(e);
      }
    }
  }

  private KafkaClientBuilder() {
  }

  public static BrokersDeclarer createProducer() {
    return new KafkaProducerBuilder();
  }

  public static GroupIdDeclarer createConsumer() {
    return new KafkaConsumerBuilder();
  }
}
