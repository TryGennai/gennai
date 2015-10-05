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

@BaseFunction.Description(name = "count")
public class Count extends BaseAggregateFunction<Long> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private long cnt;

  public Count() {
  }

  private Count(Count c) {
    super(c);
  }

  @Override
  public Count create(Object... parameters) throws ArgumentException {
    if (parameters.length > 0) {
      if (parameters.length == 1) {
        if (parameters[0] instanceof Field) {
          setParameters(parameters);
        } else {
          throw new ArgumentException("Incorrect type of argument");
        }
      } else {
        throw new ArgumentException("Incorrect number of arguments");
      }
    }
    return this;
  }

  @Override
  protected void prepare() {
    cnt = 0L;
  }

  @Override
  public Long evaluate(GungnirTuple tuple) {
    if (hasParameter()) {
      if (((Field) getParameter(0)).getValue(tuple) != null) {
        cnt++;
      }
    } else {
      cnt++;
    }
    return cnt;
  }

  @Override
  public Long exclude(GungnirTuple tuple) {
    if (hasParameter()) {
      if (getParameter(0) instanceof AggregateFunction<?>) {
        if (((AggregateFunction<?>) getParameter(0)).exclude(tuple) != null) {
          cnt--;
        }
      } else {
        if (((Field) getParameter(0)).getValue(tuple) != null) {
          cnt--;
        }
      }
    } else {
      cnt--;
    }
    return cnt;
  }

  @Override
  public void clear() {
    if (hasParameter()) {
      if (getParameter(0) instanceof AggregateFunction<?>) {
        ((AggregateFunction<?>) getParameter(0)).clear();
      }
    }
    cnt = 0L;
  }

  @Override
  public Count clone() {
    return new Count(this);
  }
}
