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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Period implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private enum Unit {
    S(TimeUnit.SECONDS),
    SEC(TimeUnit.SECONDS),
    SECONDS(TimeUnit.SECONDS),
    M(TimeUnit.MINUTES),
    MIN(TimeUnit.MINUTES),
    MINUTES(TimeUnit.MINUTES),
    H(TimeUnit.HOURS),
    HOURS(TimeUnit.HOURS),
    D(TimeUnit.DAYS),
    DAYS(TimeUnit.DAYS);

    private TimeUnit timeUnit;

    private Unit(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
    }

    TimeUnit getTimeUnit() {
      return timeUnit;
    }
  }

  private long time;
  private TimeUnit timeUnit;
  private Pattern pattern = Pattern.compile("^(\\d+)(.+)");

  private Period(long time, TimeUnit timeUnit) {
    this.time = time;
    this.timeUnit = timeUnit;
  }

  private Period(String time) {
    Matcher matcher = pattern.matcher(time);
    if (matcher.find()) {
      this.time = Long.parseLong(matcher.group(1));
      this.timeUnit = Unit.valueOf(matcher.group(2).toUpperCase()).getTimeUnit();
    } else {
      throw new IllegalArgumentException("Time format is incorrect. argument: " + time);
    }
  }

  public static Period of(long time) {
    return new Period(time, TimeUnit.SECONDS);
  }

  public static Period of(long time, TimeUnit timeUnit) {
    return new Period(time, timeUnit);
  }

  public static Period of(String time) {
    return new Period(time);
  }

  public long getTime() {
    return time;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public long toMillis() {
    return timeUnit.toMillis(time);
  }

  public int toSeconds() {
    return (int) timeUnit.toSeconds(time);
  }

  @Override
  public String toString() {
    return String.valueOf(time) + timeUnit;
  }
}
