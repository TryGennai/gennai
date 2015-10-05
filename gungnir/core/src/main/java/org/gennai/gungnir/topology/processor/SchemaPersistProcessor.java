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

package org.gennai.gungnir.topology.processor;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Map;
import java.util.Random;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.serialization.StructSerializer;
import org.gennai.gungnir.utils.KryoSerializer;
import org.gennai.gungnir.utils.kafka.HashEncoder;
import org.gennai.gungnir.utils.kafka.HashPartitioner;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SchemaPersistProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(SchemaPersistProcessor.class);

  private Schema schema;
  private transient int[] partitionFieldsIndex;
  private transient String topicName;
  private transient Producer<Integer, byte[]> producer;
  private transient Random random;
  private transient KryoSerializer serializer;

  public SchemaPersistProcessor(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    topicName = TRACKDATA_TOPIC + context.getAccountId() + "." + schema.getSchemaName();
    if (schema.getPartitionFields() != null) {
      partitionFieldsIndex = new int[schema.getPartitionFields().length];
      for (int i = 0; i < schema.getPartitionFields().length; i++) {
        partitionFieldsIndex[i] = schema.getFieldIndex(schema.getPartitionFields()[i]);
      }
    }

    if (producer == null) {
      List<String> brokers = config.getList(KAFKA_BROKERS);
      int requiredAcks = config.getInteger(KAFKA_REQUIRED_ACKS);

      producer = KafkaClientBuilder.createProducer().brokers(brokers).requiredAcks(requiredAcks)
          .keySerializer(HashEncoder.class).partitioner(HashPartitioner.class).build();
    }

    random = new Random();
    serializer = new KryoSerializer();
    serializer.register(Struct.class, new StructSerializer());

    LOG.info("SchemaPersistProcessor opened({})", this);
  }

  private int hash(List<Object> partitionKey) {
    if (partitionKey == null) {
      return random.nextInt();
    } else {
      return partitionKey.hashCode();
    }
  }

  private boolean validate(TupleValues tupleValues) {
    for (int i = 0; i < schema.getFieldCount(); i++) {
      if (schema.getFieldType(i) != null && tupleValues.getValues().get(i) != null
          && !schema.getFieldType(i).isInstance(tupleValues.getValues().get(i))) {
        return false;
      }
    }
    return false;
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    if (producer == null) {
      throw new ProcessorException("Processor isn't open");
    }

    List<KeyedMessage<Integer, byte[]>> messages = Lists.newArrayListWithCapacity(tuples.size());
    for (TupleValues tupleValues : tuples) {
      if (validate(tupleValues)) {
        List<Object> partitionKey = null;
        if (partitionFieldsIndex != null) {
          partitionKey = Lists.newArrayListWithCapacity(partitionFieldsIndex.length);
          for (int i = 0; i < partitionFieldsIndex.length; i++) {
            partitionKey.add(tupleValues.getValues().get(partitionFieldsIndex[i]));
          }
        }
        int hash = hash(partitionKey);
        byte[] bytes = serializer.serialize(tupleValues.getValues());
        messages.add(new KeyedMessage<Integer, byte[]>(topicName, hash, bytes));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Insert tracking data. topic: '{}', hash: {}, tuple: {}", topicName, hash,
              tupleValues);
        }
      } else {
        LOG.warn("Type of output field is different. tuple: {}", tupleValues);
      }
    }

    if (messages.size() > 0) {
      try {
        producer.send(messages);
      } catch (FailedToSendMessageException e) {
        throw new ProcessorException("Failed to send to '" + topicName + "'", e);
      }
    }
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }

    LOG.info("SchemaPersistProcessor closed({})", this);
  }

  @Override
  public String toString() {
    return schema.toString();
  }
}
