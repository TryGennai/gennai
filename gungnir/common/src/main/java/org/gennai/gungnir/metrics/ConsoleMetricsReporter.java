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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;

public class ConsoleMetricsReporter extends BaseMetricsReporter {

  private ConsoleReporter reporter;

  @Override
  public void start() {
    reporter = ConsoleReporter.forRegistry(getRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(getIntervalSecs(), TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    reporter.stop();
  }
}
