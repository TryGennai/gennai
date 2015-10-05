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

package org.gennai.gungnir.tuple.persistent;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import kafka.admin.AdminUtils;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.utils.ZKStringSerializer;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.serialization.StructSerializer;
import org.gennai.gungnir.utils.KryoSerializer;
import org.gennai.gungnir.utils.kafka.HashEncoder;
import org.gennai.gungnir.utils.kafka.HashPartitioner;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class KafkaPersistentEmitter extends BasePersistentEmitter {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPersistentEmitter.class);

  private Producer<Integer, byte[]> producer;
  private int maxSize;
  private KryoSerializer serializer;
  private Random random;
  private Map<String, int[]> paritionIndexesMap;
  private Set<String> existsTopics;

  public KafkaPersistentEmitter() {
    super();
  }

  private KafkaPersistentEmitter(KafkaPersistentEmitter c) {
    super(c);
    this.producer = c.producer;
    this.maxSize = c.maxSize;
  }

  @Override
  protected void prepare() {
    if (producer == null) {
      List<String> brokers = getDispatcher().getConfig().getList(KAFKA_BROKERS);
      int requiredAcks = getDispatcher().getConfig().getInteger(KAFKA_REQUIRED_ACKS);
      ProducerType producerType = ProducerType.valueOf(getDispatcher().getConfig()
          .getString(KAFKA_PRODUCER_TYPE).toUpperCase());
      producer = KafkaClientBuilder.createProducer().brokers(brokers).requiredAcks(requiredAcks)
          .keySerializer(HashEncoder.class).partitioner(HashPartitioner.class)
          .producerType(producerType).build();

      maxSize = getDispatcher().getConfig().getInteger(PERSISTENT_EMIT_TUPLES_MAX_SIZE);
    }

    serializer = new KryoSerializer();
    serializer.register(Struct.class, new StructSerializer());

    random = new Random();
  }

  @Override
  protected void sync() {
    paritionIndexesMap = Maps.newHashMap();
    existsTopics = Sets.newHashSet();
  }

  private boolean checkExistsTopic(String topicName) {
    List<String> zkServers = getDispatcher().getConfig().getList(KAFKA_ZOOKEEPER_SERVERS);
    ZkClient zkClient = new ZkClient(StringUtils.join(zkServers, ","),
        getDispatcher().getConfig().getInteger(KAFKA_ZOOKEEPER_SESSION_TIMEOUT),
        getDispatcher().getConfig().getInteger(KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT),
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

    boolean exists = AdminUtils.topicExists(zkClient, topicName);

    zkClient.close();

    return exists;
  }

  private int hash(TupleValues tupleValues) {
    int[] indexes = paritionIndexesMap.get(tupleValues.getTupleName());
    if (indexes == null) {
      Schema schema = getSchemaRegistry().get(tupleValues.getTupleName());

      if (schema.getPartitionFields() != null) {
        indexes = new int[schema.getPartitionFields().length];
        for (int i = 0; i < schema.getPartitionFields().length; i++) {
          indexes[i] = schema.getFieldIndex(schema.getPartitionFields()[i]);
        }
      } else {
        indexes = new int[0];
      }
      paritionIndexesMap.put(tupleValues.getTupleName(), indexes);
    }

    if (indexes.length > 0) {
      List<Object> partitionKey = Lists.newArrayListWithCapacity(indexes.length);
      for (int index : indexes) {
        partitionKey.add(tupleValues.getValues().get(index));
      }
      return partitionKey.hashCode();
    } else {
      return random.nextInt();
    }
  }

  @Override
  public void emit(String accountId, List<TupleValues> tuples) {
    if (producer != null) {
      List<KeyedMessage<Integer, byte[]>> messages = Lists.newArrayList();
      int size = 0;

      for (TupleValues tupleValues : tuples) {
        String topicName = TRACKDATA_TOPIC + accountId + "." + tupleValues.getTupleName();
        if (!existsTopics.contains(topicName)) {
          if (checkExistsTopic(topicName)) {
            existsTopics.add(topicName);
          } else {
            LOG.warn("Topic doesn't exist. tuple: {}", tupleValues.getTupleName());
            continue;
          }
        }

        int hash = hash(tupleValues);
        byte[] bytes = serializer.serialize(tupleValues.getValues());

        getDispatcher().getMetrics().getEmitSize().update(bytes.length);

        if (size > 0 && size + bytes.length > maxSize) {
          try {
            producer.send(messages);
          } catch (FailedToSendMessageException e) {
            LOG.error("Failed to insert tracking data", e);
          }

          getDispatcher().getMetrics().getEmitCount().mark(messages.size());
          messages.clear();
          size = 0;
        }

        messages.add(new KeyedMessage<Integer, byte[]>(topicName, hash, bytes));
        size += bytes.length;

        if (LOG.isDebugEnabled()) {
          LOG.debug("Insert tracking data. topic: '{}', hash: {}, tuple: {}", topicName, hash,
              tupleValues);
        }
      }

      try {
        producer.send(messages);
      } catch (FailedToSendMessageException e) {
        LOG.error("Failed to insert tracking data", e);
      }

      getDispatcher().getMetrics().getEmitCount().mark(messages.size());
    }
  }

  @Override
  public void cleanup() {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public KafkaPersistentEmitter clone() {
    return new KafkaPersistentEmitter(this);
  }
}
