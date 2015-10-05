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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.Struct;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class Matchers {

  private static class TupleMatcher extends TypeSafeMatcher<GungnirTuple> {

    private static final Map<Class<? extends Number>, Integer> NUMERIC_TYPES_MAP = Maps
        .newHashMap();

    static {
      NUMERIC_TYPES_MAP.put(Byte.class, 1);
      NUMERIC_TYPES_MAP.put(Short.class, 2);
      NUMERIC_TYPES_MAP.put(Integer.class, 3);
      NUMERIC_TYPES_MAP.put(Long.class, 4);
      NUMERIC_TYPES_MAP.put(Float.class, 5);
      NUMERIC_TYPES_MAP.put(Double.class, 6);
    }

    private List<Map<String, Object>> expectedTuples;

    TupleMatcher(List<Map<String, Object>> expectedTuples) {
      this.expectedTuples = expectedTuples;
    }

    private static int compareNumber(Number v1, Number v2) {
      Integer t1 = NUMERIC_TYPES_MAP.get(v1.getClass());
      Integer t2 = NUMERIC_TYPES_MAP.get(v2.getClass());
      if (t1 != null && t2 != null) {
        Integer t = (t1 > t2) ? t1 : t2;
        if (t == 5) {
          return Float.compare(((Number) v1).floatValue(), ((Number) v2).floatValue());
        } else if (t == 6) {
          return Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
        } else {
          Long l = ((Number) v1).longValue();
          return l.compareTo(((Number) v2).longValue());
        }
      } else {
        throw new IllegalArgumentException(
            "Failed to compare. Arguments isn't TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE"
                + " compare(" + v1 + ", " + v2 + ")");
      }
    }

    @SuppressWarnings("unchecked")
    private static int compare(Object v1, Object v2) {
      if (v1.getClass() == v2.getClass()) {
        if (v1 instanceof Comparable) {
          return ((Comparable<Object>) v1).compareTo(v2);
        } else {
          throw new IllegalArgumentException("Failed to compare. Arguments isn't comparable. "
              + "compare(" + v1 + ", " + v2 + ")");
        }
      } else {
        if (v1 instanceof Number && v2 instanceof Number) {
          return compareNumber((Number) v1, (Number) v2);
        } else {
          throw new IllegalArgumentException("Failed to compare. Arguments isn't same type."
              + " compare(" + v1 + ", " + v2 + ")");
        }
      }
    }

    @SuppressWarnings("unchecked")
    public static boolean compareValue(Object v1, Object v2) {
      if (v1 == null || v2 == null) {
        if (v1 == null && v2 == null) {
          return true;
        }
        return false;
      }

      if (v1 instanceof List) {
        if (!(v2 instanceof List)) {
          return false;
        }
        if (((List<Object>) v1).size() != ((List<Object>) v2).size()) {
          return false;
        }
        for (int i = 0; i < ((List<Object>) v1).size(); i++) {
          if (!compareValue(((List<Object>) v1).get(i), ((List<Object>) v2).get(i))) {
            return false;
          }
        }
      } else if (v1 instanceof Map) {
        if (!(v2 instanceof Map)) {
          return false;
        }
        if (((Map<Object, Object>) v1).size() != ((Map<Object, Object>) v2).size()) {
          return false;
        }
        Iterator<Map.Entry<Object, Object>> it = ((Map<Object, Object>) v1).entrySet().iterator();
        Iterator<Map.Entry<Object, Object>> it2 = ((Map<Object, Object>) v2).entrySet().iterator();
        for (; it.hasNext();) {
          Map.Entry<Object, Object> e1 = it.next();
          Map.Entry<Object, Object> e2 = it2.next();
          if (e1.getKey() instanceof Double && e2.getKey() instanceof String) {
            try {
              if (!compareValue(e1.getKey(), Double.parseDouble((String) e2.getKey()))) {
                return false;
              }
            } catch (NumberFormatException e) {
              return false;
            }
          } else {
            if (!compareValue(e1.getKey(), e2.getKey())) {
              return false;
            }
            if (!compareValue(e1.getValue(), e2.getValue())) {
              return false;
            }
          }
        }
      } else if (v1 instanceof Struct) {
        if (!(v2 instanceof Map)) {
          return false;
        }
        List<String> fieldNames = Lists.newArrayList(((Map<String, Object>) v2).keySet());
        List<Object> values = Lists.newArrayList(((Map<String, Object>) v2).values());
        if (!compareValue(((Struct) v1).getFieldNames(), fieldNames)) {
          return false;
        }
        if (!compareValue(((Struct) v1).getValues(), values)) {
          return false;
        }
      } else {
        if (compare(v1, v2) != 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected boolean matchesSafely(GungnirTuple tuple) {
      for (Iterator<Map<String, Object>> it = expectedTuples.iterator(); it.hasNext();) {
        Map<String, Object> expectedTuple = it.next();
        List<String> fieldNames = Lists.newArrayList(expectedTuple.keySet());
        List<Object> values = Lists.newArrayList(expectedTuple.values());
        if (fieldNames.equals(tuple.getFieldNames())) {
          if (compareValue(tuple.getTupleValues().getValues(), values)) {
            it.remove();
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Output tuple doesn't contain in expected tuples ").appendValue(
          expectedTuples);
    }
  }

  private Matchers() {
  }

  public static TupleMatcher compareTuple(List<Map<String, Object>> expectedTuples) {
    return new TupleMatcher(expectedTuples);
  }
}
