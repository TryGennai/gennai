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

import org.gennai.gungnir.topology.ArithmeticCalculator;
import org.gennai.gungnir.topology.InternalArithNode;
import org.gennai.gungnir.tuple.GungnirTuple;

@BaseFunction.Description(name = "eval")
public class Eval extends BaseFunction<Number> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Eval() {
  }

  private Eval(Eval c) {
    super(c);
  }

  @Override
  public Eval create(Object... parameters) throws ArgumentException {
    if (parameters.length == 1) {
      if (parameters[0] instanceof InternalArithNode) {
        setParameters(parameters);
      } else {
        throw new ArgumentException("Incorrect type of argument");
      }
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public Number evaluate(GungnirTuple tuple) {
    return ArithmeticCalculator.compute((InternalArithNode) getParameter(0), tuple);
  }

  @Override
  public Eval clone() {
    return new Eval(this);
  }
}
