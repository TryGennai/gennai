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

package org.gennai.gungnir.metrics;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import com.timgroup.statsd.StatsDClientException;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

public class StatsdMetricsConsumer implements IMetricsConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(StatsdMetricsConsumer.class);

  private transient String topologyName;
  private transient String statsdHost;
  private transient int statsdPort;
  private transient String statsdPrefix;

  private transient StatsDClient statsd;

  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map stormConf, Object registrationArgument,
      TopologyContext context,  IErrorReporter errorReporter) {
    @SuppressWarnings("unchecked")
    GungnirConfig conf = GungnirConfig.wrap((Map<String, Object>) stormConf.get(GUNGNIR_CONFIG));
    topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
    statsdHost = conf.getString(METRICS_STATSD_HOST);
    statsdPort = conf.getInteger(METRICS_STATSD_PORT);
    statsdPrefix = conf.getString(METRICS_CONSUMER_PREFIX);
    if (!statsdPrefix.endsWith(".")) {
      statsdPrefix += ".";
    }

    LOG.info("Metrics to StatsD[{}:{} {}]", statsdHost, statsdPort, statsdPrefix);

    try {
      statsd = new NonBlockingStatsDClient(statsdPrefix + clean(topologyName),
          statsdHost, statsdPort, new StatsDClientErrorHandler() {
        @Override
        public void handle(Exception e) {
          LOG.warn("can't send to StatsD", e);
        }
      });
    } catch (StatsDClientException e) {
      LOG.error("Failed to connect StatsD", e);
      statsd = null;
    }
  }

  private String clean(String s) {
    return s.replace('.', '_').replace('/', '_').replace(':', '.');
  }

  @Override
  public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
    if (statsd != null) {
      StringBuilder sb = new StringBuilder()
          .append(clean(taskInfo.srcWorkerHost)).append('.')
          .append(taskInfo.srcWorkerPort).append('.')
          .append(clean(taskInfo.srcComponentId)).append('.');
      int hdrLength = sb.length();

      for (DataPoint p : dataPoints) {
        sb.delete(hdrLength, sb.length());
        sb.append(p.name);
        report(sb.toString(), p.value);
      }
    }
  }

  public void report(String s, Object o) {
    LOG.debug("reporting: {}={}", s, o);
    if (o instanceof Float) {
      statsd.gauge(s, ((Float) o).doubleValue());
    } else if (o instanceof Double) {
      statsd.gauge(s, (Double) o);
    } else if (o instanceof Byte) {
      statsd.gauge(s, ((Byte) o).longValue());
    } else if (o instanceof Short) {
      statsd.gauge(s, ((Short) o).longValue());
    } else if (o instanceof Integer) {
      statsd.gauge(s, ((Integer) o).longValue());
    } else if (o instanceof Long) {
      statsd.gauge(s, (Long) o);
    } else if (o instanceof BigInteger) {
      statsd.gauge(s, ((BigInteger) o).longValue());
    } else if (o instanceof BigDecimal) {
      statsd.gauge(s, ((BigDecimal) o).doubleValue());
    } else if (o instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) o;
      StringBuilder sb = new StringBuilder(s);
      int hdrLength = sb.length();
      for (Object subName : map.keySet()) {
        sb.delete(hdrLength, sb.length());
        sb.append('.').append(clean(subName.toString()));
        report(sb.toString(), map.get(subName));
      }
    } else if (o instanceof List) {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) o;
      int i = 0;
      Iterator<Object> ite = null;
      StringBuilder sb = new StringBuilder(s);
      int hdrLength = sb.length();
      for (ite = list.iterator(); ite.hasNext();) {
        sb.delete(hdrLength, sb.length());
        sb.append('[').append(i).append(']');
        report(sb.toString(), ite.next());
        i++;
      }
    } else {
      LOG.warn("Metrics {}={}", s, o);
    }
  }

  @Override
  public void cleanup() {
    if (statsd != null) {
      statsd.stop();
    }
  }
}
