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

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.TypeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

public class MetricsManager {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);

  private MetricRegistry registry;
  private List<GungnirMetricsReporter> reporters;

  public MetricsManager(GungnirConfig config) {
    registry = new MetricRegistry();
    reporters = Lists.newArrayList();

    List<Map<String, Object>> reportersConfig = config.getList(METRICS_REPORTERS);
    if (reportersConfig != null) {
      for (Map<String, Object> conf : reportersConfig) {
        String className = (String) conf.get(METRICS_REPORTER);
        Integer intervalSecs = null;
        try {
          intervalSecs = GungnirUtils.toInt(conf.get(METRICS_INTERVAL_SECS));
        } catch (TypeCastException ignore) {
          ignore = null;
        }

        try {
          Class<?> reporterClass = Class.forName(className);
          if (GungnirMetricsReporter.class.isAssignableFrom(reporterClass)) {
            GungnirMetricsReporter reporter = (GungnirMetricsReporter) reporterClass.newInstance();
            reporter.setConfig(conf);
            reporter.setRegistry(registry);
            reporter.setIntervalSecs(intervalSecs);
            reporter.start();
            reporters.add(reporter);
          } else {
            LOG.error("Invalid reporter class '{}'", className);
          }
        } catch (ClassNotFoundException e) {
          LOG.error("Failed to start reporter", e);
        } catch (InstantiationException e) {
          LOG.error("Failed to start reporter", e);
        } catch (IllegalAccessException e) {
          LOG.error("Failed to start reporter", e);
        }
      }
    }
  }

  public MetricRegistry getRegistry() {
    return registry;
  }


  public void close() {
    for (GungnirMetricsReporter reporter : reporters) {
      reporter.stop();
    }
  }
}
