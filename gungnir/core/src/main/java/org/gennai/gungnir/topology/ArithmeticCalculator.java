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

import java.util.Map;

import org.gennai.gungnir.topology.ArithNode.Operator;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Maps;

public final class ArithmeticCalculator {

  private static final Map<Class<? extends Number>, Integer> NUMERIC_TYPES_MAP = Maps.newHashMap();

  static {
    NUMERIC_TYPES_MAP.put(Byte.class, 1);
    NUMERIC_TYPES_MAP.put(Short.class, 2);
    NUMERIC_TYPES_MAP.put(Integer.class, 3);
    NUMERIC_TYPES_MAP.put(Long.class, 4);
    NUMERIC_TYPES_MAP.put(Float.class, 5);
    NUMERIC_TYPES_MAP.put(Double.class, 6);
  }

  private ArithmeticCalculator() {
  }

  public static Number compute(Operator operatior, Number v1, Number v2) {
    Integer t1 = NUMERIC_TYPES_MAP.get(v1.getClass());
    Integer t2 = NUMERIC_TYPES_MAP.get(v2.getClass());
    if (t1 != null && t2 != null) {
      Integer t = (t1 > t2) ? t1 : t2;
      switch (t) {
        case 1:
          switch (operatior) {
            case ADDITION:
              return v1.byteValue() + v2.byteValue();
            case SUBTRACTION:
              return v1.byteValue() - v2.byteValue();
            case MULTIPLICATION:
              return v1.byteValue() * v2.byteValue();
            case DIVISION:
              return v1.doubleValue() / v2.byteValue();
            case MODULO:
              return v1.byteValue() % v2.byteValue();
            case INTEGER_DIVISION:
              return v1.byteValue() / v2.byteValue();
            default:
          }
        case 2:
          switch (operatior) {
            case ADDITION:
              return v1.shortValue() + v2.shortValue();
            case SUBTRACTION:
              return v1.shortValue() - v2.shortValue();
            case MULTIPLICATION:
              return v1.shortValue() * v2.shortValue();
            case DIVISION:
              return v1.doubleValue() / v2.shortValue();
            case MODULO:
              return v1.shortValue() % v2.shortValue();
            case INTEGER_DIVISION:
              return v1.shortValue() / v2.shortValue();
            default:
          }
        case 3:
        case 4:
          switch (operatior) {
            case ADDITION:
              return v1.longValue() + v2.longValue();
            case SUBTRACTION:
              return v1.longValue() - v2.longValue();
            case MULTIPLICATION:
              return v1.longValue() * v2.longValue();
            case DIVISION:
              return v1.doubleValue() / v2.longValue();
            case MODULO:
              return v1.longValue() % v2.longValue();
            case INTEGER_DIVISION:
              return v1.longValue() / v2.longValue();
            default:
          }
        case 5:
        case 6:
          switch (operatior) {
            case ADDITION:
              return v1.doubleValue() + v2.doubleValue();
            case SUBTRACTION:
              return v1.doubleValue() - v2.doubleValue();
            case MULTIPLICATION:
              return v1.doubleValue() * v2.doubleValue();
            case DIVISION:
              return v1.doubleValue() / v2.doubleValue();
            case MODULO:
              return v1.doubleValue() % v2.doubleValue();
            case INTEGER_DIVISION:
              Double v = v1.doubleValue() / v2.doubleValue();
              return v.longValue();
            default:
          }
        default:
      }
      return 0;
    } else {
      throw new IllegalArgumentException(
          "Failed to compute. Arguments isn't TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE"
              + " compute(" + v1 + ", " + v2 + ")");
    }
  }

  public static Number compute(InternalArithNode node, GungnirTuple tuple) {
    Number left = null;
    if (node.getLeftNode() instanceof InternalArithNode) {
      left = compute((InternalArithNode) node.getLeftNode(), tuple);
    } else {
      left = ((ExternalArithNode) node.getLeftNode()).getValue(tuple);
    }

    Number right = null;
    if (node.getRightNode() instanceof InternalArithNode) {
      right = compute((InternalArithNode) node.getRightNode(), tuple);
    } else {
      right = ((ExternalArithNode) node.getRightNode()).getValue(tuple);
    }

    try {
      Number res = compute(node.getOperatior(), left, right);
      if (res instanceof Double && (((Double) res).isInfinite() || ((Double) res).isNaN())) {
        return null;
      }
      return res;
    } catch (ArithmeticException e) {
      return null;
    }
  }
}
