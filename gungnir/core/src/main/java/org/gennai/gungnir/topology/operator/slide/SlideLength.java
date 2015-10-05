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

package org.gennai.gungnir.topology.operator.slide;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;

import org.gennai.gungnir.Period;
import org.gennai.gungnir.tuple.FieldAccessor;

public class SlideLength implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public enum LengthType {
    COUNT, TIME
  }

  private LengthType type;
  private int count;
  private Period period;
  private FieldAccessor timeField;

  public static SlideLength count(int count) {
    SlideLength slideLength = new SlideLength();
    slideLength.type = LengthType.COUNT;
    slideLength.count = count;
    return slideLength;
  }

  public static SlideLength time(Period period, FieldAccessor timeField) {
    SlideLength slideLength = new SlideLength();
    slideLength.type = LengthType.TIME;
    slideLength.period = period;
    slideLength.timeField = timeField;
    return slideLength;
  }

  public LengthType getType() {
    return type;
  }

  public int getCount() {
    return count;
  }

  public Period getPeriod() {
    return period;
  }

  public FieldAccessor getTimeField() {
    return timeField;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("length(");
    switch (type) {
      case COUNT:
        sb.append(count);
        sb.append("tuples");
        break;
      case TIME:
        sb.append(period);
        sb.append(" BY ");
        sb.append(timeField);
        break;
      default:
    }
    sb.append(')');
    return sb.toString();
  }
}
