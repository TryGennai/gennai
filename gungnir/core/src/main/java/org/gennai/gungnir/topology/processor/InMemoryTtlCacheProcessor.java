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

package org.gennai.gungnir.topology.processor;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.serialization.StructSerializer;
import org.gennai.gungnir.tuple.store.MemoryTable;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.KryoSerializer;

import com.google.common.collect.Lists;

public class InMemoryTtlCacheProcessor implements TtlCacheProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private transient MemoryTable table;
  private transient KryoSerializer serializer;

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      String tupleName, int expireSecs, int seekSize) throws ProcessorException {
    table = new MemoryTable(expireSecs, seekSize);
    serializer = new KryoSerializer();
    serializer.register(Struct.class, new StructSerializer());
  }

  @Override
  public void put(Object key, List<Object> values) throws ProcessorException {
    table.put(key, GungnirUtils.currentTimeSecs(), serializer.serialize(values));

    table.compactRange();
  }

  @Override
  public int size(Object key) {
    return table.size(key);
  }

  @Override
  public List<List<Object>> take(Object key) {
    int now = GungnirUtils.currentTimeSecs();
    List<List<Object>> valuesList = Lists.newArrayList();
    for (Iterator<MemoryTable.Entry> it = table.iterator(key); it.hasNext();) {
      MemoryTable.Entry entry = it.next();
      if (entry.getTimeKey() >= now) {
        @SuppressWarnings("unchecked")
        List<Object> values = serializer.deserialize(entry.getValue(), ArrayList.class);
        valuesList.add(values);
      }
      it.remove();
    }
    return valuesList;
  }

  @Override
  public void close() {
  }

  @Override
  public InMemoryTtlCacheProcessor clone() {
    return new InMemoryTtlCacheProcessor();
  }

  @Override
  public String toString() {
    return "memory_cache()";
  }
}
