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

import java.util.List;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Lists;

@BaseFunction.Description(name = "split")
public class Split extends BaseFunction<List<String>> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Split() {
  }

  private Split(Split c) {
    super(c);
  }

  @Override
  public Split create(Object... parameters) throws ArgumentException {
    if (parameters.length == 2) {
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
  public List<String> evaluate(GungnirTuple tuple) {
    String str = null;
    if (getParameter(0) instanceof Field) {
      Object value = ((Field) getParameter(0)).getValue(tuple);
      if (value != null) {
        str = value.toString();
      }
    } else {
      str = getParameter(0).toString();
    }

    String regex = null;
    if (getParameter(1) instanceof Field) {
      Object value = ((Field) getParameter(1)).getValue(tuple);
      if (value != null) {
        regex = value.toString();
      }
    } else {
      regex = getParameter(1).toString();
    }

    if (str != null && regex != null) {
      return Lists.newArrayList(str.split(regex));
    }
    return null;
  }

  @Override
  public Split clone() {
    return new Split(this);
  }
}
