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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class ConsumerExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerExecutor.class);

  public interface ConsumerListener {

    void onMessage(byte[] bytes);
  }

  private ConsumerConnector consumerConnector;
  private ExecutorService consumerExecutor;

  public ConsumerExecutor(ConsumerConnector consumerConnector) {
    this.consumerConnector = consumerConnector;
  }

  public void open(String topicName, final ConsumerListener listener) {
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        consumerConnector.createMessageStreams(ImmutableMap.of(topicName, 1));
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topicName);

    consumerExecutor = Executors.newSingleThreadExecutor(
        GungnirUtils.createThreadFactory("CousumerExecutor"));
    for (final KafkaStream<byte[], byte[]> stream : streams) {
      consumerExecutor.execute(new Runnable() {
        public void run() {
          for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
            byte[] bytes = msgAndMetadata.message();
            listener.onMessage(bytes);
          }
        }
      });
    }
  }

  public void close() {
    consumerExecutor.shutdown();
    LOG.info("Consumer executor shutdown.");
    consumerConnector.shutdown();
  }
}
