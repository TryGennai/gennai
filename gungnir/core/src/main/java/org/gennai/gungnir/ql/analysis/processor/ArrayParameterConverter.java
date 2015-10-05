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

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;

import org.gennai.gungnir.ql.analysis.Registry;

public class ArrayParameterConverter implements ParameterConverter {

  @Override
  public boolean canConvert(Type type) {
    if (type instanceof Class) {
      if (((Class<?>) type).isArray()) {
        return true;
      }
    } else if (type instanceof GenericArrayType) {
      return true;
    }
    return false;
  }

  @Override
  public String toTypeString(Type type, Registry registry) throws ArgmentConvertException {
    if (type instanceof Class) {
      Class<?> componentType = ((Class<?>) type).getComponentType();
      return "ARRAY<" + registry.toTypeString(componentType) + ">";
    } else if (type instanceof GenericArrayType) {
      Type componentType = ((GenericArrayType) type).getGenericComponentType();
      return "ARRAY<" + registry.toTypeString(componentType) + ">";
    }
    return null;
  }

  @Override
  public String toTypeString(Object value, Registry registry)
      throws ArgmentConvertException {
    if (!value.getClass().isArray()) {
      throw new ArgmentConvertException("Argument type mismatch. Argument is " + value);
    }
    if (Array.getLength(value) == 0) {
      throw new ArgmentConvertException("Array argument is empty.");
    }
    Object v = Array.get(value, 0);
    return "ARRAY<" + registry.toTypeString(v.getClass()) + ">";
  }

  @Override
  public Object convert(Type type, Object value) throws ArgmentConvertException {
    Class<?> componentType = ((Class<?>) type).getComponentType();
    if (!value.getClass().isArray()) {
      throw new ArgmentConvertException("Argument type mismatch. Argument is " + value);
    }
    if (Array.getLength(value) == 0) {
      throw new ArgmentConvertException("Array argument is empty.");
    }
    int len = Array.getLength(value);
    Object array = Array.newInstance(componentType, len);
    for (int i = 0; i < len; i++) {
      Array.set(array, i, Array.get(value, i));
    }
    return array;
  }
}
