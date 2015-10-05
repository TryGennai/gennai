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

package org.gennai.gungnir.ql.analysis;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.ql.analysis.processor.ArgmentConvertException;
import org.gennai.gungnir.ql.analysis.processor.ParameterConverter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Registry {

  private Map<String, Map<String, Constructor<?>>> registerMap = Maps.newHashMap();
  private List<ParameterConverter> converters = Lists.newArrayList();
  private Map<Type, ParameterConverter> convertersMap = Maps.newHashMap();

  public void addArgumentConverter(ParameterConverter converter) {
    converters.add(converter);
  }

  public String toTypeString(Type type) throws ArgmentConvertException {
    ParameterConverter converter = convertersMap.get(type);
    if (converter == null) {
      for (ParameterConverter conv : converters) {
        if (conv.canConvert(type)) {
          converter = conv;
          convertersMap.put(type, converter);
          break;
        }
      }
    }

    if (converter != null) {
      return converter.toTypeString(type, this);
    } else {
      return type.toString();
    }
  }

  private String paramsString(Constructor<?> constructor)
      throws ArgmentConvertException {
    Class<?>[] types = constructor.getParameterTypes();
    Type[] pTypes = constructor.getGenericParameterTypes();

    StringBuilder sb = new StringBuilder(128);
    for (int i = 0; i < types.length; i++) {
      if (sb.length() > 0) {
        sb.append(',');
      }

      sb.append(toTypeString(pTypes[i]));
    }
    return sb.toString();
  }

  private String toTypeString(Object value) throws ArgmentConvertException {
    Type type = value.getClass();
    ParameterConverter converter = convertersMap.get(type);
    if (converter == null) {
      for (ParameterConverter conv : converters) {
        if (conv.canConvert(type)) {
          converter = conv;
          convertersMap.put(type, converter);
          break;
        }
      }
    }

    if (converter != null) {
      return converter.toTypeString(value, this);
    } else {
      return type.toString();
    }
  }

  private String argsString(Object... args)
      throws RegisterException, ArgmentConvertException {
    if (args == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(128);
    for (Object arg : args) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append(toTypeString(arg));
    }
    return sb.toString();
  }

  protected void register(String name, Class<?> registerClass)
      throws ArgmentConvertException {
    Map<String, Constructor<?>> constructorsMap = Maps.newConcurrentMap();
    Constructor<?>[] constructors = registerClass.getConstructors();
    for (Constructor<?> constructor : constructors) {
      constructorsMap.put(paramsString(constructor), constructor);
    }
    registerMap.put(name, constructorsMap);
  }

  protected Object create(String name, Object... args)
      throws RegisterException, ArgmentConvertException {
    Map<String, Constructor<?>> constructorsMap = registerMap.get(name);
    if (constructorsMap == null) {
      throw new RegisterException("Can't found '" + name + "'");
    }

    Constructor<?> constructor = constructorsMap.get(argsString(args));
    if (constructor == null) {
      throw new RegisterException("Can't found '" + name + "(" + Arrays.toString(args) + ")'");
    }

    Class<?>[] types = constructor.getParameterTypes();
    Type[] pTypes = constructor.getGenericParameterTypes();

    for (int i = 0; i < types.length; i++) {
      ParameterConverter converter = convertersMap.get(types[i]);
      if (converter == null) {
        for (ParameterConverter conv : converters) {
          if (conv.canConvert(types[i])) {
            converter = conv;
            convertersMap.put(pTypes[i], converter);
            break;
          }
        }
      }

      if (converter != null) {
        args[i] = converter.convert(types[i], args[i]);
      }
    }

    try {
      return constructor.newInstance(args);
    } catch (Exception e) {
      throw new RegisterException("Failed to create instance '"
          + name + "(" + Arrays.toString(args) + ")'", e);
    }
  }
}
