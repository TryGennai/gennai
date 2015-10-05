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

import com.esotericsoftware.kryo.Serializer;

public final class ConcurrentKryo {

  private static class KryoThreadLocal extends ThreadLocal<KryoSerializer> {

    @Override
    protected KryoSerializer initialValue() {
      return new KryoSerializer();
    }
  }

  private static final KryoThreadLocal KRYO_THREAD_LOCAL = new KryoThreadLocal();

  private ConcurrentKryo() {
  }

  public static <T> void register(Class<T> type) {
    KRYO_THREAD_LOCAL.get().register(type);
  }

  public static <T> void register(Class<T> type, Serializer<T> serializer) {
    KRYO_THREAD_LOCAL.get().register(type, serializer);
  }

  public static byte[] serialize(Object object) {
    return KRYO_THREAD_LOCAL.get().serialize(object);
  }

  public static <T> T deserialize(byte[] bytes, Class<T> type) {
    return KRYO_THREAD_LOCAL.get().deserialize(bytes, type);
  }
}
