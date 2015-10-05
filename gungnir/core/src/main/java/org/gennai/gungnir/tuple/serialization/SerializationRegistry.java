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

package org.gennai.gungnir.tuple.serialization;

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;

import backtype.storm.Config;

import com.esotericsoftware.kryo.Serializer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class SerializationRegistry {

  private SerializationRegistry() {
  }

  private static <T> void register(List<Map<String, String>> serializations,
      Class<T> targetClass, Class<? extends Serializer<T>> serializerClass) {
    Map<String, String> serMap = Maps.newLinkedHashMap();
    serMap.put(targetClass.getName(), serializerClass.getName());
    serializations.add(serMap);
  }

  public static void putConf(Config stormConf) {
    List<Map<String, String>> serializations = Lists.newArrayList();

    register(serializations, TupleValues.class, TupleValuesSerializer.class);
    register(serializations, Struct.class, StructSerializer.class);

    stormConf.put(Config.TOPOLOGY_KRYO_REGISTER, serializations);
  }
}
