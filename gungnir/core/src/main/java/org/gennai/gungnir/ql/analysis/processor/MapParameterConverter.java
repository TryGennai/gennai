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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import org.gennai.gungnir.ql.analysis.Registry;

public class MapParameterConverter implements ParameterConverter {

  @Override
  public boolean canConvert(Type type) {
    if (type instanceof Class) {
      return Map.class.isAssignableFrom((Class<?>) type);
    } else if (type instanceof ParameterizedType) {
      ParameterizedType pType = (ParameterizedType) type;
      return Map.class.isAssignableFrom((Class<?>) pType.getRawType());
    }
    return false;
  }

  @Override
  public String toTypeString(Type type, Registry registry) throws ArgmentConvertException {
    if (type instanceof Class) {
      return "MAP";
    } else if (type instanceof ParameterizedType) {
      ParameterizedType pType = (ParameterizedType) type;
      return "MAP<" + registry.toTypeString(pType.getActualTypeArguments()[0]) + ", "
          + registry.toTypeString(pType.getActualTypeArguments()[1]) + ">";
    }
    return null;
  }

  public String toTypeString(Object value, Registry registry)
      throws ArgmentConvertException {
    Map<?, ?> map = (Map<?, ?>) value;
    Map.Entry<?, ?> entry = (Map.Entry<?, ?>) map.entrySet().iterator().next();
    if (entry != null) {
      return "MAP<" + registry.toTypeString(entry.getKey().getClass()) + ", "
          + registry.toTypeString(entry.getValue().getClass()) + ">";
    }
    return null;
  }

  @Override
  public Object convert(Type type, Object value) throws ArgmentConvertException {
    return value;
  }
}
