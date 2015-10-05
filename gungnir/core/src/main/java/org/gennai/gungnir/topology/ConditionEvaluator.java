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

package org.gennai.gungnir.topology;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import org.gennai.gungnir.tuple.ComplexCondition;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public final class ConditionEvaluator implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(ConditionEvaluator.class);

  private static final Map<Class<? extends Number>, Integer> NUMERIC_TYPES_MAP = Maps.newHashMap();

  static {
    NUMERIC_TYPES_MAP.put(Byte.class, 1);
    NUMERIC_TYPES_MAP.put(Short.class, 2);
    NUMERIC_TYPES_MAP.put(Integer.class, 3);
    NUMERIC_TYPES_MAP.put(Long.class, 4);
    NUMERIC_TYPES_MAP.put(Float.class, 5);
    NUMERIC_TYPES_MAP.put(Double.class, 6);
  }

  private ConditionEvaluator() {
  }

  public static String likeRegex(String likeString) {
    StringBuilder sb = new StringBuilder(likeString);
    sb.insert(0, '^');
    sb.append('$');
    int len = sb.length();
    int us = 0;
    for (int i = 0; i < len;) {
      switch (sb.charAt(i)) {
        case '_':
          us++;
          i++;
          break;
        case '%':
          if (us > 0) {
            String n = String.valueOf(us);
            int l = n.length();
            sb.delete(i - us, i);
            sb.insert(i - us, ".{");
            sb.insert(i - us + 2, n);
            sb.insert(i - us + 2 + l, '}');
            i += -us + 3 + l;
            len += -us + 3 + l;
            us = 0;
          }
          sb.replace(i, i + 1, ".*");
          i += 2;
          len++;
          break;
        default:
          if (us > 0) {
            String n = String.valueOf(us);
            int l = n.length();
            sb.delete(i - us, i);
            sb.insert(i - us, ".{");
            sb.insert(i - us + 2, n);
            sb.insert(i - us + 2 + l, '}');
            i += -us + 3 + l;
            len += -us + 3 + l;
            us = 0;
          }
          i++;
      }
    }
    return sb.toString();
  }

  public static int compareNumber(Number v1, Number v2) {
    Integer t1 = NUMERIC_TYPES_MAP.get(v1.getClass());
    Integer t2 = NUMERIC_TYPES_MAP.get(v2.getClass());
    if (t1 != null && t2 != null) {
      Integer t = (t1 > t2) ? t1 : t2;
      if (t == 5) {
        return Float.compare(v1.floatValue(), v2.floatValue());
      } else if (t == 6) {
        return Double.compare(v1.doubleValue(), v2.doubleValue());
      } else {
        Long l = v1.longValue();
        return l.compareTo(v2.longValue());
      }
    } else {
      throw new IllegalArgumentException(
          "Failed to compare. Arguments isn't TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE"
              + " compare(" + v1 + ", " + v2 + ")");
    }
  }

  @SuppressWarnings("unchecked")
  public static int compare(Object v1, Object v2) {
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

  // TODO No11
  private static boolean compare(SimpleCondition.Type type, Object v1, Object v2) {
    switch (type) {
      case IS_NULL:
        return (v1 == null);
      case IS_NOT_NULL:
        return (v1 != null);
      default:
        if (v1 == null) {
          return false;
        }
    }

    if (v1 instanceof Collection) {
      switch (type) {
        case ALL:
          Collection<?> values = (Collection<?>) v1;
          if (v2.getClass().isArray()) {
            int len = Array.getLength(v2);
            for (int i = 0; i < len; i++) {
              boolean ret = false;
              for (Object value : values) {
                if (compare(value, Array.get(v2, i)) == 0) {
                  ret = true;
                  break;
                }
              }
              if (!ret) {
                return false;
              }
            }
            return true;
          } else {
            for (Object value : values) {
              if (compare(value, v2) == 0) {
                return true;
              }
            }
            return false;
          }
        case IN:
          values = (Collection<?>) v1;
          if (v2.getClass().isArray()) {
            int len = Array.getLength(v2);
            for (int i = 0; i < len; i++) {
              for (Object value : values) {
                if (compare(value, Array.get(v2, i)) == 0) {
                  return true;
                }
              }
            }
          } else {
            for (Object value : values) {
              if (compare(value, v2) == 0) {
                return true;
              }
            }
          }
          return false;
        default:
          return false;
      }
    } else {
      try {
        switch (type) {
          case EQ:
            if (compare(v1, v2) == 0) {
              return true;
            }
            break;
          case NE:
            if (compare(v1, v2) != 0) {
              return true;
            }
            break;
          case GT:
            if (compare(v1, v2) > 0) {
              return true;
            }
            break;
          case GE:
            if (compare(v1, v2) >= 0) {
              return true;
            }
            break;
          case LT:
            if (compare(v1, v2) < 0) {
              return true;
            }
            break;
          case LE:
            if (compare(v1, v2) <= 0) {
              return true;
            }
            break;
          case LIKE:
            if (v1 instanceof String) {
              if (((String) v1).matches(likeRegex((String) v2))) {
                return true;
              }
            }
            break;
          case REGEXP:
            if (v1 instanceof String) {
              String regex = ((String) v2);
              if (((String) v1).matches(regex)) {
                return true;
              }
            }
            break;
          case IN:
            if (v2.getClass().isArray()) {
              int len = Array.getLength(v2);
              for (int i = 0; i < len; i++) {
                if (compare(v1, Array.get(v2, i)) == 0) {
                  return true;
                }
              }
            } else {
              if (compare(v1, v2) == 0) {
                return true;
              }
            }
            break;
          case BETWEEN:
            Object from = Array.get(v2, 0);
            Object to = Array.get(v2, 1);
            if (compare(v1, from) >= 0 && compare(v1, to) <= 0) {
              return true;
            }
            break;
          default:
            return false;
        }
        return false;
      } catch (IllegalArgumentException e) {
        if (LOG.isInfoEnabled()) {
          LOG.info(e.getMessage());
        }
        return false;
      }
    }
  }

  public static boolean isKeep(Condition condition, GungnirTuple tuple) {
    if (condition instanceof ComplexCondition) {
      ComplexCondition complexCondition = (ComplexCondition) condition;
      switch (complexCondition.getType()) {
        case AND:
          for (Condition cond : complexCondition.getConditions()) {
            if (!isKeep(cond, tuple)) {
              return false;
            }
          }
          return true;
        case OR:
          for (Condition cond : complexCondition.getConditions()) {
            if (isKeep(cond, tuple)) {
              return true;
            }
          }
          return false;
        case NOT:
          for (Condition cond : complexCondition.getConditions()) {
            if (isKeep(cond, tuple)) {
              return false;
            }
          }
          return true;
        default:
          return false;
      }
    } else {
      SimpleCondition simpleCondition = (SimpleCondition) condition;
      Object v1 = simpleCondition.getField().getValue(tuple);
      Object v2 = null;
      if (simpleCondition.getValue() instanceof Field) {
        v2 = ((Field) simpleCondition.getValue()).getValue(tuple);
        if (v2 == null) {
          return false;
        }
      } else {
        v2 = simpleCondition.getValue();
      }

      return compare(simpleCondition.getType(), v1, v2);
    }
  }
}
