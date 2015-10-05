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

@BaseFunction.Description(name = "sqrt")
public class Sqrt extends BaseFunction<Double> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Sqrt() {
  }

  private Sqrt(Sqrt c) {
    super(c);
  }

  @Override
  public Function<Double> create(Object... parameters) throws ArgumentException {
    if (parameters.length == 1) {
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
    Object value = null;
    if (getParameter(0) instanceof Field) {
      value = ((Field) getParameter(0)).getValue(tuple);
    } else {
      value = getParameter(0);
    }

    try {
      return Math.sqrt(GungnirUtils.toDouble(value));
    } catch (TypeCastException e) {
      return 0.0;
    }
  }

  @Override
  public Function<Double> clone() {
    return new Sqrt(this);
  }
}
