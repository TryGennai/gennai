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

import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.json.StructSerializer;
import org.gennai.gungnir.utils.kafka.KafkaClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class KafkaEmitProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaEmitProcessor.class);

  private static final String KAFKA_EMIT_BROKERS = "kafka.emit.brokers";
  private static final String KAFKA_EMIT_REQUIRED_ACKS = "kafka.emit.required.acks";

  private enum Format {
    JSON, CSV
  }

  private String topicName;
  private Format format;
  private transient Map<String, List<String>> outputFieldNames;
  private transient Producer<Integer, byte[]> producer;
  private transient ObjectMapper mapper;
  private transient SimpleDateFormat sdf;

  public KafkaEmitProcessor(String topicName) {
    this.topicName = topicName;
    this.format = Format.JSON;
  }

  public KafkaEmitProcessor(String topicName, String format) {
    this.topicName = topicName;
    try {
      this.format = Format.valueOf(format.toUpperCase());
    } catch (IllegalArgumentException e) {
      this.format = Format.JSON;
    }
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    topicName = context.replaceVariable(topicName);
    this.outputFieldNames = outputFieldNames;

    List<String> brokers = config.getList(KAFKA_EMIT_BROKERS);
    int requiredAcks = config.getInteger(KAFKA_EMIT_REQUIRED_ACKS);

    producer =
        KafkaClientBuilder.createProducer().brokers(brokers).requiredAcks(requiredAcks).build();

    SimpleModule module = new SimpleModule("GungnirModule",
        new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null, null, null));
    module.addSerializer(Struct.class, new StructSerializer());

    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

    if (format == Format.JSON) {
      mapper = new ObjectMapper();
      mapper.setDateFormat(sdf);
      mapper.registerModule(module);
    }

    LOG.info("KafkaEmitProcessor opened({})", this);
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    if (producer == null) {
      throw new ProcessorException("Processor isn't open");
    }

    List<KeyedMessage<Integer, byte[]>> messages = Lists.newArrayListWithCapacity(tuples.size());
    StringBuilder sb = null;

    for (TupleValues tupleValues : tuples) {
      if (format == Format.CSV) {
        if (sb == null) {
          sb = new StringBuilder();
        } else {
          sb.setLength(0);
        }

        for (Object value : tupleValues.getValues()) {
          if (sb.length() > 0) {
            sb.append(',');
          }
          if (value instanceof String) {
            String str = value.toString();
            str = str.replace("\"", "\"\"");
            if (str.indexOf(',') >= 0) {
              sb.append('"');
              sb.append(str);
              sb.append('"');
            } else {
              sb.append(str);
            }
          } else if (value instanceof Number) {
            sb.append(value);
          } else if (value instanceof Boolean) {
            sb.append(value);
          } else if (value instanceof Date) {
            sb.append(sdf.format((Date) value));
          } else if (value instanceof List) {
            String str = value.toString();
            str = str.replace("\"", "\"\"");
            sb.append('"');
            sb.append(str);
            sb.append('"');
          } else if (value instanceof Map) {
            String str = value.toString();
            str = str.replace("\"", "\"\"");
            sb.append('"');
            sb.append(str);
            sb.append('"');
          } else if (value instanceof Struct) {
            String str = value.toString();
            str = str.replace("\"", "\"\"");
            sb.append('"');
            sb.append(str);
            sb.append('"');
          }
        }

        messages.add(new KeyedMessage<Integer, byte[]>(topicName, sb.toString().getBytes()));
      } else {
        List<String> fieldNames = outputFieldNames.get(tupleValues.getTupleName());
        if (fieldNames.size() > 0) {
          Map<String, Object> record = Maps.newLinkedHashMap();
          for (int i = 0; i < fieldNames.size(); i++) {
            record.put(fieldNames.get(i), tupleValues.getValues().get(i));
          }

          byte[] bytes = null;
          try {
            bytes = mapper.writeValueAsBytes(record);
          } catch (JsonGenerationException e) {
            throw new ProcessorException("Failed to convert json format", e);
          } catch (JsonMappingException e) {
            throw new ProcessorException("Failed to convert json format", e);
          } catch (IOException e) {
            throw new ProcessorException("Failed to convert json format", e);
          }

          messages.add(new KeyedMessage<Integer, byte[]>(topicName, bytes));

          if (LOG.isDebugEnabled()) {
            LOG.debug("Emit to '{}' {}", topicName, record);
          }
        }
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

    LOG.info("KafkaEmitProcessor closed({})", this);
  }

  @Override
  public String toString() {
    return "kafka_emit(" + topicName + ")";
  }
}
