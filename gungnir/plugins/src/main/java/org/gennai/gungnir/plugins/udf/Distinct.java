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

import java.util.Map;

import org.apache.storm.guava.collect.Maps;

public class Distinct {

  private Map<Object, Integer> valuesMap = Maps.newHashMap();

  public Object evaluate(Object value) {
    if (value != null) {
      Integer cnt = valuesMap.get(value);
      if (cnt != null) {
        cnt++;
      } else {
        cnt = 1;
      }
      valuesMap.put(value, cnt);
      if (cnt == 1) {
        return value;
      } else {
        return null;
      }
    }
    return null;
  }

  public Object exclude(Object value) {
    if (value != null) {
      Integer cnt = valuesMap.get(value);
      if (cnt != null) {
        cnt--;
        if (cnt > 0) {
          valuesMap.put(value, cnt);
        } else {
          valuesMap.remove(value);
          return value;
        }
      }
    }
    return null;
  }

  public void clear() {
    valuesMap = Maps.newHashMap();
  }
}
