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

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class StatsdMetricsReporter extends BaseMetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(StatsdMetricsReporter.class);

  private StatsdReporter reporter;

  private static final String[] SERVER_INFO =
      ManagementFactory.getRuntimeMXBean().getName().split("@");

  @Override
  public void start() {
    Map<String, Object> conf = getConfig();
    String statsdHost = (String) conf.get(METRICS_REPORTER_STATSD_HOST);
    int statsdPort = Utils.getInt(conf.get(METRICS_REPORTER_STATSD_PORT));
    StringBuilder prefix = new StringBuilder();
    prefix.append(conf.get(METRICS_REPORTER_STATSD_PREFIX));
    if (prefix.charAt(prefix.length() - 1) != '.') {
      prefix.append('.');
    }
    prefix.append(SERVER_INFO[1]).append('.').append(SERVER_INFO[0]);

    LOG.info("Metrics to StatsD[{}:{} {}]", statsdHost, statsdPort, prefix);

    reporter = StatsdReporter.forRegistry(getRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .prefixedWith(prefix.toString())
        .build(statsdHost, statsdPort);
    if (reporter != null) {
      reporter.start(getIntervalSecs(), TimeUnit.SECONDS);
    }
  }

  @Override
  public void stop() {
    if (reporter != null) {
      reporter.stop();
    }
  }
}
