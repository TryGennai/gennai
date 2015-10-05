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

package org.gennai.gungnir.utils;

import java.lang.reflect.Constructor;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.google.common.collect.Maps;

public class KryoSerializer {

  private Kryo kryo = new Kryo();
  private Output output = new Output(2048, 100 * 1240 * 1024);
  private Input input = new Input();

  @SuppressWarnings("rawtypes")
  private static class TreeMapSerializer extends MapSerializer {

    @Override
    public void write(Kryo kryo, Output output, Map map) {
      kryo.writeClassAndObject(output, ((TreeMap) map).comparator());
      super.write(kryo, output, map);
    }

    @Override
    protected Map create(Kryo kryo, Input input, Class<Map> type) {
      return createTreeMap(type, (Comparator) kryo.readClassAndObject(input));
    }

    @Override
    protected Map createCopy(Kryo kryo, Map original) {
      return createTreeMap(original.getClass(), ((TreeMap) original).comparator());
    }

    // https://github.com/EsotericSoftware/kryo/issues/166
    @SuppressWarnings("unchecked")
    private Map createTreeMap(Class<? extends Map> type, Comparator comparator) {
      if (type != TreeMap.class && type != null) {
        try {
          Constructor constructor = type.getConstructor(Comparator.class);
          if (!constructor.isAccessible()) {
            try {
              constructor.setAccessible(true);
            } catch (SecurityException se) {
              se = null;
            }
          }
          return (TreeMap) constructor.newInstance(comparator);
        } catch (Exception ex) {
          throw new KryoException(ex);
        }
      }
      return Maps.newTreeMap(comparator);
    }
  }

  public KryoSerializer() {
    kryo.register(TreeMap.class, new TreeMapSerializer());
  }

  public <T> void register(Class<T> type) {
    kryo.register(type);
  }

  public <T> void register(Class<T> type, Serializer<T> serializer) {
    kryo.register(type, serializer);
  }

  public byte[] serialize(Object object, boolean classInclusive) {
    output.clear();
    if (classInclusive) {
      kryo.writeClassAndObject(output, object);
    } else {
      kryo.writeObject(output, object);
    }
    return output.toBytes();
  }

  public byte[] serialize(Object object) {
    return serialize(object, false);
  }

  public <T> T deserialize(byte[] bytes, Class<T> type) {
    input.setBuffer(bytes);
    return kryo.readObject(input, type);
  }

  public Object deserialize(byte[] bytes) {
    input.setBuffer(bytes);
    return kryo.readClassAndObject(input);
  }
}
