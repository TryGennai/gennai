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

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class FetchStore {

  private Map<String, List<FieldAccessor>> keyFieldsMap = Maps.newHashMap();
  private Map<String, Map<String, List<List<Object>>>> fetchMap = Maps.newHashMap();

  public void put(String operatorName, String[] keyFieldNames,
      Map<String, List<List<Object>>> fetchValuesMap) {
    List<FieldAccessor> keyFields = Lists.newArrayListWithCapacity(keyFieldNames.length);
    for (String fieldName : keyFieldNames) {
      String[] names = fieldName.split("\\.");
      FieldAccessor field = null;
      for (String name : names) {
        if (field == null) {
          field = new FieldAccessor(name);
        } else {
          field = new FieldAccessor(name, field);
        }
      }
      keyFields.add(field);
    }
    keyFieldsMap.put(operatorName, keyFields);
    fetchMap.put(operatorName, fetchValuesMap);
  }

  public List<List<Object>> find(String operatorName, GungnirTuple tuple) {
    List<FieldAccessor> keyFields = keyFieldsMap.get(operatorName);
    StringBuilder sb = new StringBuilder();
    for (FieldAccessor field : keyFields) {
      if (sb.length() > 0) {
        sb.append('+');
      }
      sb.append(field.getValue(tuple));
    }

    List<List<Object>> valuesList = fetchMap.get(operatorName).get(sb.toString());
    if (valuesList == null) {
      valuesList = Lists.newArrayList();
    }
    return valuesList;
  }
}
