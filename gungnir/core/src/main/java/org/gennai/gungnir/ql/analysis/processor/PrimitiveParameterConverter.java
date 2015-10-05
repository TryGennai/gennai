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

package org.gennai.gungnir.ql.analysis.processor;

import java.lang.reflect.Type;

import org.gennai.gungnir.ql.analysis.Registry;

public class PrimitiveParameterConverter implements ParameterConverter {

  @Override
  public boolean canConvert(Type type) {
    return type == Byte.class || type == Short.class || type == Integer.class
        || type == Long.class || type == Float.class || type == Double.class
        || type == Boolean.class;
  }

  @Override
  public String toTypeString(Type type, Registry registry) {
    if (type == Byte.class) {
      return byte.class.getName();
    } else if (type == Short.class) {
      return short.class.getName();
    } else if (type == Integer.class) {
      return int.class.getName();
    } else if (type == Long.class) {
      return long.class.getName();
    } else if (type == Float.class) {
      return float.class.getName();
    } else if (type == Double.class) {
      return double.class.getName();
    } else if (type == Boolean.class) {
      return boolean.class.getName();
    }
    return null;
  }

  @Override
  public String toTypeString(Object value, Registry registry) {
    return toTypeString(value.getClass(), registry);
  }

  @Override
  public Object convert(Type type, Object value) throws ArgmentConvertException {
    return value;
  }
}
