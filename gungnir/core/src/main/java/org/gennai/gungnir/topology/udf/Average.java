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

@BaseFunction.Description(name = "avg")
public class Average extends BaseAggregateFunction<Number> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(Average.class);

  private Object total;
  private long cnt;

  public Average() {
  }

  private Average(Average c) {
    super(c);
  }

  @Override
  public Average create(Object... parameters) throws ArgumentException {
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
    cnt = 0L;
  }

  @Override
  public Number evaluate(GungnirTuple tuple) {
    Object value = null;
    if (getParameter(0) instanceof Field) {
      value = ((Field) getParameter(0)).getValue(tuple);
    } else {
      value = getParameter(0);
    }

    if (value != null) {
      try {
        total = GungnirUtils.addition(total, value);
        cnt++;
      } catch (ArithmeticOperationException e) {
        LOG.warn("Failed to added {} + {}", total, value);
      }
    }

    if (total instanceof Double) {
      Double d = ((Double) total);
      return d / cnt;
    } else {
      Long l = ((Long) total);
      return l / cnt;
    }
  }

  @Override
  public Number exclude(GungnirTuple tuple) {
    Object value = null;
    if (getParameter(0) instanceof AggregateFunction<?>) {
      value = ((AggregateFunction<?>) getParameter(0)).exclude(tuple);
    } else if (getParameter(0) instanceof Field) {
      value = ((Field) getParameter(0)).getValue(tuple);
    } else {
      value = getParameter(0);
    }


    if (value != null) {
      try {
        total = GungnirUtils.subtraction(total, value);
        cnt--;
      } catch (ArithmeticOperationException e) {
        LOG.warn("Failed to subtracted {} - {}", total, value);
      }
    }

    if (cnt == 0) {
      return 0;
    }

    if (total instanceof Double) {
      Double d = ((Double) total);
      return d / cnt;
    } else {
      Long l = ((Long) total);
      return l / cnt;
    }
  }

  @Override
  public void clear() {
    if (getParameter(0) instanceof AggregateFunction<?>) {
      ((AggregateFunction<?>) getParameter(0)).clear();
    }
    total = 0L;
    cnt = 0L;
  }

  @Override
  public Average clone() {
    return new Average(this);
  }
}
