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
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.TypeCastException;

@BaseFunction.Description(name = "distance")
public class Distance extends BaseFunction<Double> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Distance() {
  }

  private Distance(Distance c) {
    super(c);
  }

  @Override
  public Function<Double> create(Object... parameters) throws ArgumentException {
    if (parameters.length == 4) {
      setParameters(parameters);
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public Double evaluate(GungnirTuple tuple) {
    Double[] coords = new Double[4];
    for (int i = 0; i < coords.length; i++) {
      Object value = null;
      if (getParameter(i) instanceof Field) {
        value = ((Field) getParameter(i)).getValue(tuple);
      } else {
        value = getParameter(i);
      }

      try {
        coords[i] = GungnirUtils.toDouble(value);
      } catch (TypeCastException e) {
        return 0.0;
      }
    }

    return Math.sqrt((coords[2] - coords[0]) * (coords[2] - coords[0])
        + (coords[3] - coords[1]) * (coords[3] - coords[1]));
  }

  @Override
  public Function<Double> clone() {
    return new Distance(this);
  }
}
