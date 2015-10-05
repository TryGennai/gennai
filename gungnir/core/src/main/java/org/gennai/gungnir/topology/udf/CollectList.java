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

@BaseFunction.Description(name = "collect_list")
public class CollectList extends BaseAggregateFunction<List<?>> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<Object> values;

  public CollectList() {
  }

  private CollectList(CollectList c) {
    super(c);
  }

  @Override
  public CollectList create(Object... parameters) throws ArgumentException {
    if (parameters.length == 1) {
      if (parameters[0] instanceof Field) {
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
    values = Lists.newArrayList();
  }

  @Override
  public List<?> evaluate(GungnirTuple tuple) {
    Object value = ((Field) getParameter(0)).getValue(tuple);
    if (value != null) {
      values.add(value);
    }
    return Lists.newArrayList(values);
  }

  @Override
  public List<?> exclude(GungnirTuple tuple) {
    values.remove(0);
    return  Lists.newArrayList(values);
  }

  @Override
  public void clear() {
    values = Lists.newArrayList();
  }

  @Override
  public CollectList clone() {
    return new CollectList(this);
  }
}
