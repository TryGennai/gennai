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
import static org.gennai.gungnir.tuple.schema.PrimitiveType.*;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.tuple.schema.PrimitiveType;
import org.gennai.gungnir.tuple.schema.TimestampType;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.TypeCastException;

@BaseFunction.Description(name = "cast")
public class Cast extends BaseFunction<Object> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public Cast() {
  }

  private Cast(Cast c) {
    super(c);
  }

  @Override
  public Cast create(Object... parameters) throws ArgumentException {
    if (parameters.length == 2) {
      setParameters(parameters);
      if (parameters[0] instanceof Field) {
        setAliasName(((Field) parameters[0]).getFieldName());
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
  public Object evaluate(GungnirTuple tuple) {
    Object value = null;
    if (getParameter(0) instanceof Field) {
      value = ((Field) getParameter(0)).getValue(tuple);
    } else {
      value = getParameter(0);
    }

    FieldType fieldType = null;
    if (getParameter(1) instanceof FieldType) {
      if (((FieldType) getParameter(1)) instanceof PrimitiveType
          || ((FieldType) getParameter(1)) instanceof TimestampType) {
        fieldType = (FieldType) getParameter(1);
      }
    }

    if (value != null && fieldType != null) {
      try {
        if (fieldType.equals(STRING)) {
          return value.toString();
        } else if (fieldType.equals(TINYINT)) {
          return GungnirUtils.toTinyint(value);
        } else if (fieldType.equals(SMALLINT)) {
          return GungnirUtils.toSmallint(value);
        } else if (fieldType.equals(INT)) {
          return GungnirUtils.toInt(value);
        } else if (fieldType.equals(BIGINT)) {
          return GungnirUtils.toBigint(value);
        } else if (fieldType.equals(FLOAT)) {
          return GungnirUtils.toFloat(value);
        } else if (fieldType.equals(DOUBLE)) {
          return GungnirUtils.toDouble(value);
        } else if (fieldType.equals(BOOLEAN)) {
          return GungnirUtils.toBoolean(value);
        } else if (fieldType instanceof TimestampType) {
          String dateFormat = ((TimestampType) fieldType).getDateFormat();
          return GungnirUtils.toTimestamp(value, dateFormat);
        }
      } catch (TypeCastException e) {
        return null;
      }
    }
    return null;
  }

  @Override
  public Cast clone() {
    return new Cast(this);
  }
}
