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

package org.gennai.gungnir.plugins.udf;

public class Sum {

  private Object total = 0L;

  public Object evaluate(Byte value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d += value;
      total = d;
    } else {
      Long l = ((Long) total);
      l += value;
      total = l;
    }
    return total;
  }

  public Object evaluate(Short value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d += value;
      total = d;
    } else {
      Long l = ((Long) total);
      l += value;
      total = l;
    }
    return total;
  }

  public Object evaluate(Integer value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d += value;
      total = d;
    } else {
      Long l = ((Long) total);
      l += value;
      total = l;
    }
    return total;
  }

  public Object evaluate(Long value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d += value;
      total = d;
    } else {
      Long l = ((Long) total);
      l += value;
      total = l;
    }
    return total;
  }

  public Object evaluate(Float value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d += value;
      total = d;
    } else {
      Double d = ((Long) total).doubleValue();
      d += value;
      total = d;
    }
    return total;
  }

  public Object evaluate(Double value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d += value;
      total = d;
    } else {
      Double d = ((Long) total).doubleValue();
      d += value;
      total = d;
    }
    return total;
  }

  public Object evaluate(Object value) {
    return total;
  }

  public Object exclude(Byte value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d -= value;
      total = d;
    } else {
      Long l = ((Long) total);
      l -= value;
      total = l;
    }
    return total;
  }

  public Object exclude(Short value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d -= value;
      total = d;
    } else {
      Long l = ((Long) total);
      l -= value;
      total = l;
    }
    return total;
  }

  public Object exclude(Integer value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d -= value;
      total = d;
    } else {
      Long l = ((Long) total);
      l -= value;
      total = l;
    }
    return total;
  }

  public Object exclude(Long value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d -= value;
      total = d;
    } else {
      Long l = ((Long) total);
      l -= value;
      total = l;
    }
    return total;
  }

  public Object exclude(Float value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d -= value;
      total = d;
    } else {
      Double d = ((Long) total).doubleValue();
      d -= value;
      total = d;
    }
    return total;
  }

  public Object exclude(Double value) {
    if (total instanceof Double) {
      Double d = ((Double) total);
      d -= value;
      total = d;
    } else {
      Double d = ((Long) total).doubleValue();
      d -= value;
      total = d;
    }
    return total;
  }

  public Object exclude(Object value) {
    return total;
  }

  public void clear() {
    total = 0L;
  }
}
