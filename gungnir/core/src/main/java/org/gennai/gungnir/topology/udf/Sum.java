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

package org.gennai.gungnir.topology.udf;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.utils.ArithmeticOperationException;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BaseFunction.Description(name = "sum")
public class Sum extends BaseAggregateFunction<Object> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(Sum.class);

  private Object total;

  public Sum() {
  }

  protected Sum(Sum c) {
    super(c);
  }

  @Override
  public Sum create(Object... parameters) throws ArgumentException {
    if (parameters.length == 1) {
      setParameters(parameters);
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
    total = 0L;
  }

  @Override
  public Object evaluate(GungnirTuple tuple) {
    Object value = null;
    try {
      if (getParameter(0) instanceof Field) {
        value = ((Field) getParameter(0)).getValue(tuple);
      } else {
        value = getParameter(0);
      }
      total = GungnirUtils.addition(total, value);
    } catch (ArithmeticOperationException e) {
      LOG.warn("Failed to added {} + {}", total, value);
    }
    return total;
  }

  @Override
  public Object exclude(GungnirTuple tuple) {
    Object value = null;
    try {
      if (getParameter(0) instanceof AggregateFunction<?>) {
        value = ((AggregateFunction<?>) getParameter(0)).exclude(tuple);
      } else if (getParameter(0) instanceof Field) {
        value = ((Field) getParameter(0)).getValue(tuple);
      } else {
        value = getParameter(0);
      }
      total = GungnirUtils.subtraction(total, value);
    } catch (ArithmeticOperationException e) {
      LOG.warn("Failed to subtracted {} - {}", total, value);
    }
    return total;
  }

  @Override
  public void clear() {
    if (getParameter(0) instanceof AggregateFunction<?>) {
      ((AggregateFunction<?>) getParameter(0)).clear();
    }
    total = 0L;
  }

  @Override
  public Sum clone() {
    return new Sum(this);
  }
}
