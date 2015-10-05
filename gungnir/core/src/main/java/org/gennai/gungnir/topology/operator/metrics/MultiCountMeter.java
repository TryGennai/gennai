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

package org.gennai.gungnir.topology.operator.metrics;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Map;

import com.google.common.collect.Maps;

public class MultiCountMeter implements Metrics {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Map<String, CountMeter> valuesMap = Maps.newLinkedHashMap();

  public CountMeter scope(String key) {
    CountMeter value = valuesMap.get(key);
    if (value == null) {
      value = new CountMeter();
      valuesMap.put(key, value);
    }
    return value;
  }

  @Override
  public Object getValueAndReset() {
    Map<String, Object> ret = Maps.newLinkedHashMap();
    for (Map.Entry<String, CountMeter> entry : valuesMap.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().getValueAndReset());
    }
    return ret;
  }
}
