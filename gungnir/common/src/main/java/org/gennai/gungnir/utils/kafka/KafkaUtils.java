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

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;

import org.apache.curator.framework.CuratorFramework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

@Deprecated
public final class KafkaUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int SO_TIMEOUT = 100000;
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final String CLIENT_ID = "leaderLookup";

  private KafkaUtils() {
  }

  public static List<TopicMetadata> getTopicMetaData(CuratorFramework curator, String topicName)
      throws Exception {
    List<TopicMetadata> topicMetadata = Lists.newArrayList();

    List<String> ids = curator.getChildren().forPath(ZkUtils.BrokerIdsPath());
    for (String id : ids) {
      byte[] bytes = curator.getData().forPath(ZkUtils.BrokerIdsPath() + "/" + id);
      JsonNode infoNode = MAPPER.readTree(bytes);
      String host = infoNode.get("host").asText();
      int port = infoNode.get("port").asInt();

      SimpleConsumer consumer = new SimpleConsumer(host, port, SO_TIMEOUT, BUFFER_SIZE, CLIENT_ID);

      TopicMetadataRequest req = new TopicMetadataRequest(Lists.newArrayList(topicName));
      TopicMetadataResponse res = consumer.send(req);

      topicMetadata.addAll(res.topicsMetadata());

      consumer.close();
    }

    return topicMetadata;
  }
}
