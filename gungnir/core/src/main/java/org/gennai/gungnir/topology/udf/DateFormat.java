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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;

@BaseFunction.Description(name = "date_format")
public class DateFormat extends BaseFunction<Object> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private SimpleDateFormat sdf;

  public DateFormat() {
  }

  private DateFormat(DateFormat c) {
    super(c);
  }

  @Override
  public DateFormat create(Object... parameters) throws ArgumentException {
    if (parameters.length == 2) {
      setParameters(parameters);
      if (parameters[0] instanceof Field) {
        setAliasName(((Field) parameters[0]).getFieldName());
      } else {
        throw new ArgumentException("Incorrect type of argument");
      }
      try {
        new SimpleDateFormat(parameters[1].toString());
      } catch (IllegalArgumentException e) {
        throw new ArgumentException("Incorrect type of argument");
      }
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
    sdf = new SimpleDateFormat(getParameter(1).toString());
  }

  @Override
  public Object evaluate(GungnirTuple tuple) {
    Object value = null;
    if (getParameter(0) instanceof Field) {
      value = ((Field) getParameter(0)).getValue(tuple);
    } else {
      value = getParameter(0);
    }

    if (value != null && value instanceof Date) {
      return sdf.format((Date) value);
    } else {
      return value;
    }
  }

  @Override
  public DateFormat clone() {
    return new DateFormat(this);
  }
}
