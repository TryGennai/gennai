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

package org.gennai.gungnir;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.TypeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import backtype.storm.utils.Utils;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public final class GungnirConfig extends LinkedHashMap<String, Object> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(GungnirConfig.class);

  public static final String GUNGNIR_SERVER_PORT = "gungnir.server.port";
  public static final String SESSION_TIMEOUT_SECS = "session.timeout.secs";
  public static final String COMMAND_PROCESSOR_CACHE_SIZE = "command.processor.cache.size";
  public static final String GUNGNIR_NODE_PATH = "gungnir.node.path";
  public static final String TUPLE_STORE_SERVER_PORT = "tuple.store.server.port";
  public static final String TRACKING_COOKIE_MAXAGE = "tracking.cookie.maxage";
  public static final String PERSISTENT_DESER_QUEUE_SIZE = "persistent.deser.queue.size";
  public static final String PERSISTENT_DESER_PARALLELISM = "persistent.deser.parallelism";
  public static final String PERSISTENT_DESERIALIZER = "persistent.deserializer";
  public static final String PERSISTENT_EMITTER_QUEUE_SIZE = "persistent.emitter.queue.size";
  public static final String PERSISTENT_EMITTER_PARALLELISM = "persistent.emitter.parallelism";
  public static final String PERSISTENT_EMIT_TUPLES_MAX = "persistent.emit.tuples.max";
  public static final String PERSISTENT_EMIT_TUPLES_MAX_SIZE = "persistent.emit.tuples.max.size";
  public static final String PERSISTENT_EMITTER = "persistent.emitter";
  public static final String REWRITE_RULES = "rewrite.rules";
  public static final String REWRITE_PATTERN = "pattern";
  public static final String REWRITE_TARGET = "target";
  public static final String CLUSTER_MODE = "cluster.mode";
  public static final String LOCAL_CLUSTER = "local";
  public static final String DISTRIBUTED_CLUSTER = "distributed";
  public static final String CLUSTER_ZOOKEEPER_SERVERS = "cluster.zookeeper.servers";
  public static final String CLUSTER_ZOOKEEPER_SESSION_TIMEOUT =
      "cluster.zookeeper.session.timeout";
  public static final String CLUSTER_ZOOKEEPER_CONNECTION_TIMEOUT =
      "cluster.zookeeper.connection.timeout";
  public static final String CLUSTER_ZOOKEEPER_RETRY_TIMES = "cluster.zookeeper.retry.times";
  public static final String CLUSTER_ZOOKEEPER_RETRY_INTERVAL = "cluster.zookeeper.retry.interval";

  public static final String GUNGNIR_ADMIN_SERVER_PORT = "gungnir.admin.server.port";
  public static final String GUNGNIR_ADMIN_SERVER_BACKLOG = "gungnir.admin.server.backlog";

  public static final String STORM_CLUSTER_MODE = "storm.cluster.mode";
  public static final String STORM_NIMBUS_HOST = "storm.nimbus.host";

  public static final String TOPOLOGY_WORKERS = "topology.workers";
  public static final String TOPOLOGY_STATUS_CHECK_TIMES = "topology.status.check.times";
  public static final String TOPOLOGY_STATUS_CHECK_INTERVAL = "topology.status.check.interval";
  public static final String DEFAULT_PARALLELISM = "default.parallelism";
  public static final String LOCAL_DIR = "gungnir.local.dir";

  public static final String METASTORE = "metastore";
  public static final String METASTORE_MONGODB_SERVERS = "metastore.mongodb.servers";
  public static final String METASTORE_NAME = "metastore.name";

  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_REQUIRED_ACKS = "kafka.required.acks";
  public static final String KAFKA_PRODUCER_TYPE = "kafka.producer.type";
  public static final String KAFKA_AUTO_COMMIT_INTERVAL = "kafka.auto.commit.interval";
  public static final String KAFKA_ZOOKEEPER_SERVERS = "kafka.zookeeper.servers";
  public static final String KAFKA_ZOOKEEPER_SESSION_TIMEOUT = "kafka.zookeeper.session.timeout";
  public static final String KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT =
      "kafka.zookeeper.connection.timeout";
  public static final String KAFKA_ZOOKEEPER_SYNC_TIMEOUT = "kafka.zookeeper.sync.timeout";
  public static final String KAFKA_ZOOKEEPER_RETRY_TIMES = "kafka.zookeeper.retry.times";
  public static final String KAFKA_ZOOKEEPER_RETRY_INTERVAL = "kafka.zookeeper.retry.interval";

  public static final String EXPORT_RETRY_TIMES = "export.retry.times";
  public static final String EXPORT_RETRY_INTERVAL = "export.retry.interval";

  public static final String COMPONENT_SNAPSHOT_QUEUE_SIZE = "component.snapshot.queue.size";
  public static final String COMPONENT_SNAPSHOT_PARALLELISM = "component.snapshot.parallelism";

  public static final String SPOUT_OPERATOR_QUEUE_SIZE = "spout.operator.queue.size";
  public static final String EMIT_OPERATOR_QUEUE_SIZE = "emit.operator.queue.size";
  public static final String EMIT_OPERATOR_EMIT_TUPLES_MAX = "emit.operator.emit.tuples.max";
  public static final String TUPLEJOIN_SEEK_SIZE = "tuplejoin.seek.size";

  public static final String LOG_APPEND_QUEUE_SIZE = "log.append.queue.size";
  public static final String LOG_APPEND_RECONNECT_DELAY = "log.append.reconnect.delay";

  public static final String DEBUG_ENABLED = "debug";
  public static final String LOG_RECEIVER_HOST = "log.receiver.host";
  public static final String LOG_RECEIVER_PORT = "log.receiver.port";
  public static final String DEBUG_LOG_APPEND_QUEUE_SIZE = "debug.log.append.queue.size";
  public static final String DEBUG_LOG_APPEND_RECONNECT_DELAY = "debug.log.append.reconnect.delay";

  public static final String METRICS_REPORTERS = "metrics.reporters";
  public static final String METRICS_REPORTER = "reporter";
  public static final String METRICS_INTERVAL_SECS = "interval.secs";
  public static final String METRICS_REPORTER_STATSD_HOST = "statsd.host";
  public static final String METRICS_REPORTER_STATSD_PORT = "statsd.port";
  public static final String METRICS_REPORTER_STATSD_PREFIX = "statsd.prefix";
  public static final String TOPOLOGY_METRICS_ENABLED = "topology.metrics.enabled";
  public static final String TOPOLOGY_METRICS_CONSUMER = "topology.metrics.consumer";
  public static final String TOPOLOGY_METRICS_CONSUMER_PARALLELISM =
      "topology.metrics.consumer.parallelism";
  public static final String TOPOLOGY_METRICS_INTERVAL_SECS = "topology.metrics.interval.secs";
  public static final String TOPOLOGY_STATS_SAMPLE_RATE = "topology.stats.sample.rate";
  public static final String METRICS_STATSD_HOST = "metrics.statsd.host";
  public static final String METRICS_STATSD_PORT = "metrics.statsd.port";
  public static final String METRICS_CONSUMER_PREFIX = "metrics.consumer.prefix";
  public static final String METRICS_REPORTER_PREFIX = "metrics.reporter.prefix";

  public static final String GUNGNIR_SERVER_HOST = "gungnir.server.host";
  public static final String TUPLE_STORE_SERVER_HOST = "tuple.store.server.host";
  public static final String GUNGNIR_CLIENT_RESPONSE_TIMEOUT = "gungnir.client.response.timeout";
  public static final String LOG_BUFFER_MAX = "log.buffer.max";

  public static final String CLASS_PATH = "gungnir.class.path";

  private static final String DEFAULTS_CONFIG_FILE = "gungnir-defaults.yaml";
  private static final String CONFIG_FILE = "gungnir.yaml";
  private static final String GUNGNIR_CONFIG_FILE = "gungnir.conf.file";

  private GungnirConfig() {
  }

  private GungnirConfig(Map<String, Object> config) {
    super(config);
  }

  @SuppressWarnings("unchecked")
  private Object clone(Object value) {
    if (value instanceof List) {
      List<Object> copy = Lists.newArrayList();
      for (Object v : (List<Object>) value) {
        copy.add(clone(v));
      }
      return copy;
    } else if (value instanceof Map) {
      Map<String, Object> copy = Maps.newLinkedHashMap();
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
        copy.put(entry.getKey(), clone(entry.getValue()));
      }
      return copy;
    } else {
      return value;
    }
  }

  private GungnirConfig(GungnirConfig c) {
    for (Map.Entry<String, Object> entry : c.entrySet()) {
      put(entry.getKey(), clone(entry.getValue()));
    }
  }

  @SuppressWarnings("unchecked")
  public static GungnirConfig readGugnirConfig() {
    GungnirConfig config = new GungnirConfig();
    config.putAll(Utils.findAndReadConfigFile(DEFAULTS_CONFIG_FILE, true));
    String confFile = System.getProperty(GUNGNIR_CONFIG_FILE);
    if (confFile == null || confFile.isEmpty()) {
      config.putAll(Utils.findAndReadConfigFile(CONFIG_FILE, false));
    } else {
      BufferedReader reader = null;
      try {
        reader = Files.newReader(new File(confFile), Charsets.UTF_8);

        Yaml yaml = new Yaml(new SafeConstructor());
        Map<String, Object> ret = (Map<String, Object>) yaml.load(reader);
        if (ret != null) {
          config.putAll(ret);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read " + confFile);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.warn("Failed to close " + confFile);
          }
        }
      }
    }

    Map<String, Object> stormConf = Utils.readStormConfig();
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      if (entry.getValue() instanceof String) {
        String value = (String) entry.getValue();
        if (value.startsWith("${") && value.endsWith("}")) {
          String key = value.substring(2, value.length() - 1);
          config.put(entry.getKey(), stormConf.get(key));
        }
      }
    }

    return config;
  }

  public static GungnirConfig wrap(Map<String, Object> config) {
    return new GungnirConfig(config);
  }

  public String getString(String key) {
    return (String) get(key);
  }

  public Byte getByte(String key) {
    Object value = get(key);
    if (value != null) {
      try {
        return GungnirUtils.toTinyint(value);
      } catch (TypeCastException ignore) {
        ignore = null;
      }
    }
    return null;
  }

  public Short getShort(String key) {
    Object value = get(key);
    if (value != null) {
      try {
        return GungnirUtils.toSmallint(value);
      } catch (TypeCastException ignore) {
        ignore = null;
      }
    }
    return null;
  }

  public Integer getInteger(String key) {
    Object value = get(key);
    if (value != null) {
      try {
        return GungnirUtils.toInt(value);
      } catch (TypeCastException ignore) {
        ignore = null;
      }
    }
    return null;
  }

  public Long getLong(String key) {
    Object value = get(key);
    if (value != null) {
      try {
        return GungnirUtils.toBigint(value);
      } catch (TypeCastException ignore) {
        ignore = null;
      }
    }
    return null;
  }

  public Float getFloat(String key) {
    Object value = get(key);
    if (value != null) {
      try {
        return GungnirUtils.toFloat(value);
      } catch (TypeCastException ignore) {
        ignore = null;
      }
    }
    return null;
  }

  public Double getDouble(String key) {
    Object value = get(key);
    if (value != null) {
      try {
        return GungnirUtils.toDouble(value);
      } catch (TypeCastException ignore) {
        ignore = null;
      }
    }
    return null;
  }

  public Boolean getBoolean(String key) {
    Object value = get(key);
    if (value != null) {
      return GungnirUtils.toBoolean(value);
    }
    return false;
  }

  public byte[] getBinary(String key) {
    return (byte[]) get(key);
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getList(String key) {
    return (List<T>) get(key);
  }

  public InetSocketAddress getAddress(String key) {
    Object value = get(key);
    if (value != null) {
      if (value instanceof String) {
        String address = (String) value;
        if (!address.isEmpty()) {
          int index = address.indexOf(':');
          if (index > 0) {
            String host = address.substring(0, index);
            int port = Integer.valueOf(address.substring(index + 1));
            return new InetSocketAddress(host, port);
          }
        }
      } else if (value instanceof Integer) {
        return new InetSocketAddress((Integer) value);
      }
    }
    return null;
  }

  public Class<?> getClass(String key) throws ClassNotFoundException {
    String className = getString(key);
    return Class.forName(className);
  }

  @Override
  public GungnirConfig clone() {
    return new GungnirConfig(this);
  }
}
