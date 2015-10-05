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

@BaseFunction.Description(name = "slice")
public class Slice extends BaseFunction<List<?>> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Slice() {
  }

  private Slice(Slice c) {
    super(c);
  }

  @Override
  public Slice create(Object... parameters) throws ArgumentException {
    if (parameters.length == 2 || parameters.length == 3) {
      setParameters(parameters);
      if (parameters[0] instanceof Field) {
        setAliasName(((Field) parameters[0]).getFieldName());
      } else {
        throw new ArgumentException("Incorrect type of argument");
      }
      if (!(parameters[1] instanceof Field || parameters[1] instanceof Integer)) {
        throw new ArgumentException("Incorrect type of argument");
      }
      if (parameters.length == 3) {
        if (!(parameters[2] instanceof Field || parameters[2] instanceof Integer)) {
          throw new ArgumentException("Incorrect type of argument");
        }
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
  public List<?> evaluate(GungnirTuple tuple) {
    Object value = ((Field) getParameter(0)).getValue(tuple);
    if (value instanceof List) {
      int size = ((List<?>) value).size();

      int fromIndex = 0;
      if (getParameter(1) instanceof Field) {
        Object index = ((Field) getParameter(1)).getValue(tuple);
        if (index instanceof Number) {
          fromIndex = ((Number) index).intValue();
        }
      } else {
        fromIndex = (Integer) getParameter(1);
      }
      if (fromIndex < 0) {
        fromIndex = size + fromIndex;
      }
      if (fromIndex < 0) {
        fromIndex = 0;
      }
      if (fromIndex > size) {
        fromIndex = size;
      }

      int toIndex = size;
      if (getParameter(2) != null) {
        if (getParameter(2) instanceof Field) {
          Object index = ((Field) getParameter(2)).getValue(tuple);
          if (index instanceof Number) {
            toIndex = ((Number) index).intValue();
          }
        } else {
          toIndex = (Integer) getParameter(2);
        }
      }
      if (toIndex < 0) {
        toIndex = size + toIndex;
      }
      if (toIndex < 0) {
        toIndex = 0;
      }
      if (toIndex > size) {
        toIndex = size;
      }

      if (fromIndex > toIndex) {
        return Lists.newArrayList();
      }

      return Lists.newArrayList(((List<?>) value).subList(fromIndex, toIndex));
    }

    return null;
  }

  @Override
  public Slice clone() {
    return new Slice(this);
  }
}
