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

import static org.gennai.gungnir.topology.processor.KafkaSpoutProcessor2.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.topology.processor.spout.TupleAndMessageId;
import org.gennai.gungnir.topology.processor.spout.TupleDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.KafkaUtils;
import storm.kafka.Partition;
import storm.kafka.SpoutConfig;
import storm.kafka.trident.MaxMetric;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class PartitionManager {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

  private SpoutConfig spoutConfig;
  private ZkState state;
  private DynamicPartitionConnections connections;
  private Partition partition;
  private SimpleConsumer consumer;
  private int retryTimes;
  private int retryInterval;
  private String commitPath;
  private Long commitOffset;
  private Long startOffset;
  private TupleDeserializer deserializer;
  private SortedSet<Long> pendingOffsets = Sets.newTreeSet();
  private SortedSet<Long> failedOffsets = Sets.newTreeSet();
  private long numAcked;
  private long numFailed;

  private CombinedMetric fetchLatencyMax;
  private ReducedMetric fetchLatencyMean;
  private CountMetric fetchCount;
  private CountMetric fetchMessageCount;

  private long getOffset(long offsetTime) throws ProcessorException {
    TopicAndPartition topicAndPartition =
        new TopicAndPartition(spoutConfig.topic, partition.partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = Maps.newHashMap();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(offsetTime, 1));
    OffsetRequest request =
        new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
            consumer.clientId());

    try {
      for (int i = 0; i < retryTimes; i++) {
        OffsetResponse response = consumer.getOffsetsBefore(request);
        short error = response.errorCode(spoutConfig.topic, partition.partition);
        if (error == ErrorMapping.NoError()) {
          long[] offsets = response.offsets(spoutConfig.topic, partition.partition);
          if (offsets.length > 0) {
            return offsets[0];
          } else {
            throw new ProcessorException("Failed to get offset. topic: " + spoutConfig.topic
                + ", partition: " + partition.partition);
          }
        } else {
          if (error != ErrorMapping.UnknownTopicOrPartitionCode()) {
            throw new ProcessorException("Failed to get offset. topic: " + spoutConfig.topic
                + ", partition: " + partition.partition);
          }
        }

        TimeUnit.MILLISECONDS.sleep(retryInterval);
      }
    } catch (InterruptedException ignore) {
      ignore = null;
    }

    throw new ProcessorException("Failed to get offset. topic: " + spoutConfig.topic
        + ", partition: " + partition.partition);
  }

  public PartitionManager(GungnirConfig config, SpoutConfig spoutConfig, ZkState state,
      DynamicPartitionConnections connections, String topologyId, Partition partition,
      TupleDeserializer deserializer)
      throws ProcessorException {
    this.spoutConfig = spoutConfig;
    this.state = state;
    this.connections = connections;
    this.partition = partition;
    this.deserializer = deserializer;

    consumer = connections.register(partition.host, partition.partition);

    retryTimes = config.getInteger(PARTITION_OPERATION_RETRY_TIMES);
    retryInterval = config.getInteger(PARTITION_OPERATION_RETRY_INTERVAL);

    commitPath = spoutConfig.zkRoot + "/" + spoutConfig.id + "/" + spoutConfig.topic + "/"
        + partition.partition;
    try {
      Map<String, Object> stateNode = state.readJSON(commitPath);
      if (stateNode != null) {
        commitOffset = ((Number) stateNode.get("offset")).longValue();
      }
    } catch (Exception e) {
      LOG.warn("Failed to read state", e);
    }

    long earliestOffset = getOffset(kafka.api.OffsetRequest.EarliestTime());
    long latestOffset = getOffset(kafka.api.OffsetRequest.LatestTime());
    if (spoutConfig.forceFromStart) {
      if (spoutConfig.startOffsetTime == kafka.api.OffsetRequest.EarliestTime()) {
        commitOffset = earliestOffset;
      } else {
        commitOffset = latestOffset;
      }
    } else {
      if (spoutConfig.startOffsetTime == kafka.api.OffsetRequest.EarliestTime()) {
        if (commitOffset == null) {
          commitOffset = earliestOffset;
        }
      } else {
        if (commitOffset == null || latestOffset - commitOffset > spoutConfig.maxOffsetBehind) {
          commitOffset = latestOffset;
        }
      }

      if (commitOffset < earliestOffset) {
        commitOffset = earliestOffset;
      }
    }

    startOffset = commitOffset;

    fetchLatencyMax = new CombinedMetric(new MaxMetric());
    fetchLatencyMean = new ReducedMetric(new MeanReducer());
    fetchCount = new CountMetric();
    fetchMessageCount = new CountMetric();
  }

  public Map<String, Object> getMetricsDataMap() {
    Map<String, Object> metricsMap = Maps.newHashMap();
    metricsMap.put("fetchLatencyMax/" + spoutConfig.topic + "/" + partition.partition,
        fetchLatencyMax.getValueAndReset());
    metricsMap.put("fetchLatencyMean/" + spoutConfig.topic + "/" + partition.partition,
        fetchLatencyMean.getValueAndReset());
    metricsMap.put("fetchCount" + spoutConfig.topic + "/" + partition.partition,
        fetchCount.getValueAndReset());
    metricsMap.put("fetchMessageCount" + spoutConfig.topic + "/" + partition.partition,
        fetchMessageCount.getValueAndReset());
    return metricsMap;
  }

  private List<MessageAndOffset> fetch() {
    boolean hasFailed = !failedOffsets.isEmpty();
    long offset;
    if (hasFailed) {
      offset = failedOffsets.first();
    } else {
      offset = startOffset;
    }

    ByteBufferMessageSet messageSet = null;
    long start = System.nanoTime();

    messageSet = KafkaUtils.fetchMessages(spoutConfig, consumer, partition, offset);

    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    fetchLatencyMax.update(elapsed);
    fetchLatencyMean.update(elapsed);
    fetchCount.incr();

    if (messageSet != null) {
      List<MessageAndOffset> messages = Lists.newArrayList();
      int numMessages = 0;

      for (MessageAndOffset message : messageSet) {
        long curOffset = message.offset();
        if (curOffset < offset) {
          continue;
        }
        if (!hasFailed || failedOffsets.contains(curOffset)) {
          numMessages++;
          pendingOffsets.add(curOffset);
          messages.add(message);
          startOffset = Math.max(message.nextOffset(), startOffset);
          if (hasFailed) {
            failedOffsets.remove(curOffset);
          }
        }
      }

      fetchMessageCount.incrBy(numMessages);

      return messages;
    }

    return null;
  }

  public List<TupleAndMessageId> next() {
    List<MessageAndOffset> messages = fetch();
    if (messages == null || messages.isEmpty()) {
      return null;
    }

    List<TupleAndMessageId> tupleAndMessageIds = null;
    for (MessageAndOffset message : messages) {
      ByteBuffer payload = message.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      List<Object> values = deserializer.deserialize(bytes);
      if (values != null) {
        if (tupleAndMessageIds == null) {
          tupleAndMessageIds = Lists.newArrayList();
        }
        tupleAndMessageIds.add(new TupleAndMessageId(values,
            new KafkaMessageId(partition, message.offset())));
      }
    }

    return tupleAndMessageIds;
  }

  public void ack(Long offset) {
    if (!pendingOffsets.isEmpty()
        && pendingOffsets.first() < offset - spoutConfig.maxOffsetBehind) {
      LOG.info("Truncate pending offsets. pending offsets first: {}", pendingOffsets.first());
      pendingOffsets.headSet(offset - spoutConfig.maxOffsetBehind).clear();
    }
    pendingOffsets.remove(offset);
    numAcked++;
  }

  public void fail(Long offset) {
    if (offset < startOffset - spoutConfig.maxOffsetBehind) {
      LOG.info("Skip failed tuples. offset: {}, startOffset: {}", offset, startOffset);
    } else {
      failedOffsets.add(offset);
      numFailed++;
      if (numAcked == 0 && numFailed > spoutConfig.maxOffsetBehind) {
        LOG.error("Many tuples failed. The number of failures is {}", numFailed);
      }
    }
  }

  public void commit() {
    long offset;
    if (pendingOffsets.isEmpty()) {
      offset = startOffset;
    } else {
      offset = pendingOffsets.first();
    }

    if (commitOffset != offset) {
      Map<Object, Object> stateNode = ImmutableMap.builder()
          .put("offset", offset)
          .put("partition", partition.partition)
          .put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port))
          .put("topic", spoutConfig.topic).build();
      try {
        state.writeJSON(commitPath, stateNode);
      } catch (Exception e) {
        LOG.error("Failed to write offset", e);
      }

      commitOffset = offset;
    }
  }

  public void close() {
    connections.unregister(partition.host, partition.partition);
  }

  @Override
  public String toString() {
    return this.partition.toString();
  }
}
