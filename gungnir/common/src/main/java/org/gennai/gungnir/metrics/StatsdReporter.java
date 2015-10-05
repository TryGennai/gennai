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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import com.timgroup.statsd.StatsDClientException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StatsdReporter extends ScheduledReporter {

  private static final Logger LOG = LoggerFactory.getLogger(StatsdReporter.class);

  private final StatsDClient statsd;
  private final String prefix;

  private StatsdReporter(MetricRegistry registry, StatsDClient statsd,
      String prefix, TimeUnit rate, TimeUnit duration, MetricFilter filter) {
    super(registry, "statsd-reporter", filter, rate, duration);
    this.statsd = statsd;
    this.prefix = prefix;
  }

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static final class Builder {
    private final MetricRegistry registry;
    private String prefix;
    private TimeUnit rate;
    private TimeUnit duration;
    private MetricFilter filter;
    private StatsDClient statsd;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.prefix = null;
      this.rate = TimeUnit.SECONDS;
      this.duration = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    public Builder prefixedWith(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder convertRatesTo(TimeUnit rate) {
      this.rate = rate;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit duration) {
      this.duration = duration;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public StatsdReporter build(String host, int port) {
      try {
        statsd =
            new NonBlockingStatsDClient(prefix, host, port, new StatsDClientErrorHandler() {
              @Override
              public void handle(Exception e) {
                LOG.warn("can't send to StatsD", e);
              }
            });
      } catch (StatsDClientException e) {
        LOG.error("Failed to connect StatsD [{}:{}]", host, port, e);
        return null;
      }
      return build(statsd);
    }

    public StatsdReporter build(final StatsDClient statsd) {
      return new StatsdReporter(registry, statsd, prefix, rate, duration, filter);
    }
  }

  @Override
  public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

    for (@SuppressWarnings("rawtypes") Entry<String, Gauge> entry : gauges.entrySet()) {
      reportGauge(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      reportCounter(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      reportHistogram(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      reportMetere(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      reportTimer(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      if (statsd != null) {
        statsd.stop();
      }
    }
  }

  private void reportTimer(String name, Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();
    if (statsd != null) {
      statsd.gauge(prefix(name, "count"), timer.getCount());
      statsd.gauge(prefix(name, "mean"), convertDuration(snapshot.getMean()));
      statsd.gauge(prefix(name, "min"), convertDuration(snapshot.getMin()));
      statsd.gauge(prefix(name, "stddev"), convertDuration(snapshot.getStdDev()));
      statsd.gauge(prefix(name, "p50"), convertDuration(snapshot.getMedian()));
      statsd.gauge(prefix(name, "p75"), convertDuration(snapshot.get75thPercentile()));
      statsd.gauge(prefix(name, "p95"), convertDuration(snapshot.get95thPercentile()));
      statsd.gauge(prefix(name, "p98"), convertDuration(snapshot.get98thPercentile()));
      statsd.gauge(prefix(name, "p99"), convertDuration(snapshot.get99thPercentile()));
      statsd.gauge(prefix(name, "p999"), convertDuration(snapshot.get999thPercentile()));
      statsd.gauge(prefix(name, "mean_rate"), convertRate(timer.getMeanRate()));
      statsd.gauge(prefix(name, "m1_rate"), convertRate(timer.getOneMinuteRate()));
      statsd.gauge(prefix(name, "m5_rate"), convertRate(timer.getFiveMinuteRate()));
      statsd.gauge(prefix(name, "m15_rate"), convertRate(timer.getFifteenMinuteRate()));
    }
    reportMetere(name, timer);
  }

  private void reportMetere(String name, Metered meter) {
    if (statsd != null) {
      statsd.gauge(prefix(name, "count"), meter.getCount());
      statsd.gauge(prefix(name, "mean_rate"), convertRate(meter.getMeanRate()));
      statsd.gauge(prefix(name, "m1_rate"), convertRate(meter.getOneMinuteRate()));
      statsd.gauge(prefix(name, "m5_rate"), convertRate(meter.getFiveMinuteRate()));
      statsd.gauge(prefix(name, "m15_rate"), convertRate(meter.getFifteenMinuteRate()));
    }
  }

  private void reportHistogram(String name, Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();
    if (statsd != null) {
      statsd.gauge(prefix(name, "count"), histogram.getCount());
      statsd.gauge(prefix(name, "max"), snapshot.getMax());
      statsd.gauge(prefix(name, "mean"), snapshot.getMean());
      statsd.gauge(prefix(name, "min"), snapshot.getMin());
      statsd.gauge(prefix(name, "stddev"), snapshot.getStdDev());
      statsd.gauge(prefix(name, "p50"), snapshot.getMedian());
      statsd.gauge(prefix(name, "p75"), snapshot.get75thPercentile());
      statsd.gauge(prefix(name, "p95"), snapshot.get95thPercentile());
      statsd.gauge(prefix(name, "p98"), snapshot.get98thPercentile());
      statsd.gauge(prefix(name, "p99"), snapshot.get99thPercentile());
      statsd.gauge(prefix(name, "p999"), snapshot.get999thPercentile());
    }
}

  private void reportCounter(String name, Counter counter) {
    if (statsd != null) {
      statsd.gauge(prefix(name), counter.getCount());
    }
  }

  private void reportGauge(String name, Gauge<?> gauge) {
    Object value = gauge.getValue();
    if (statsd != null && value != null) {
      if (value instanceof Float) {
        statsd.gauge(prefix(name), ((Float) value).doubleValue());
      } else if (value instanceof Double) {
        statsd.gauge(prefix(name), ((Double) value).doubleValue());
      } else if (value instanceof Byte) {
        statsd.gauge(prefix(name), ((Byte) value).longValue());
      } else if (value instanceof Short) {
        statsd.gauge(prefix(name), ((Short) value).longValue());
      } else if (value instanceof Integer) {
        statsd.gauge(prefix(name), ((Integer) value).longValue());
      } else if (value instanceof Long) {
        statsd.gauge(prefix(name), ((Long) value).longValue());
      } else if (value instanceof BigInteger) {
        statsd.gauge(prefix(name), ((BigInteger) value).longValue());
      } else if (value instanceof BigDecimal) {
        statsd.gauge(prefix(name), ((BigDecimal) value).doubleValue());
      }
    }
  }

  private String prefix(String... components) {
    return MetricRegistry.name(prefix, components);
  }
}
