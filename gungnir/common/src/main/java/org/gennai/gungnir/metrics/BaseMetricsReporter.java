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

import java.util.Map;

import com.codahale.metrics.MetricRegistry;

public abstract class BaseMetricsReporter implements GungnirMetricsReporter {

  private Map<String, Object> config;
  private MetricRegistry registry;
  private int intervalSecs;

  @Override
  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  protected Map<String, Object> getConfig() {
    return config;
  }

  @Override
  public void setRegistry(MetricRegistry registry) {
    this.registry = registry;
  }

  protected MetricRegistry getRegistry() {
    return registry;
  }

  @Override
  public void setIntervalSecs(int intervalSecs) {
    this.intervalSecs = intervalSecs;
  }

  protected int getIntervalSecs() {
    return intervalSecs;
  }
}
