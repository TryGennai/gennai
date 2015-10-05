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

package org.gennai.gungnir.topology.operator.snapshot;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;

import org.gennai.gungnir.Period;
import org.quartz.CronExpression;

public final class SnapshotInterval implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public enum IntervalType {
    COUNT, TIME, CRON
  }

  private IntervalType type;
  private int count;
  private Period period;
  private String schedulingPattern;

  private SnapshotInterval() {
  }

  public static SnapshotInterval count(int count) {
    SnapshotInterval interval = new SnapshotInterval();
    interval.type = IntervalType.COUNT;
    interval.count = count;
    return interval;
  }

  public static SnapshotInterval time(Period period) {
    SnapshotInterval interval = new SnapshotInterval();
    interval.type = IntervalType.TIME;
    interval.period = period;
    return interval;
  }

  public static SnapshotInterval cron(String schedulingPattern) {
    SnapshotInterval interval = new SnapshotInterval();
    interval.type = IntervalType.CRON;
    interval.schedulingPattern = schedulingPattern;
    return interval;
  }

  public IntervalType getType() {
    return type;
  }

  public int getCount() {
    return count;
  }

  public Period getPeriod() {
    return period;
  }

  public String getSchedulingPattern() {
    return schedulingPattern;
  }

  public boolean validateSchedulingPattern() {
    getSchedulingPattern();
    if (schedulingPattern != null) {
      return CronExpression.isValidExpression(schedulingPattern);
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("interval(");
    switch (type) {
      case COUNT:
        sb.append(count);
        sb.append("tuples");
        break;
      case TIME:
        sb.append(period);
        break;
      case CRON:
        sb.append(schedulingPattern);
        break;
      default:
    }
    sb.append(')');
    return sb.toString();
  }
}
