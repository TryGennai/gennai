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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.serialization.StructSerializer;
import org.gennai.gungnir.tuple.store.RocksDBTable;
import org.gennai.gungnir.tuple.store.RocksDBTable.EntryIterator;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.KryoSerializer;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class FileTtlCacheProcessor implements TtlCacheProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(FileTtlCacheProcessor.class);

  private transient RocksDBTable table;
  private transient KryoSerializer serializer;

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      String tupleName, int expireSecs, int seekSize) throws ProcessorException {
    try {
      String dbPath = config.getString(LOCAL_DIR) + "/" + CACHE_DIR + "/" + context.getTopologyId()
          + "/" + operatorContext.getName() + "_" + operatorContext.getId() + "/" + tupleName;
      Files.createDirectories(Paths.get(dbPath));

      table = RocksDBTable.open(dbPath + "/" + context.getComponent().getTopologyContext()
          .getThisTaskIndex(), expireSecs, seekSize);
    } catch (RocksDBException e) {
      throw new ProcessorException(e);
    } catch (IOException e) {
      throw new ProcessorException(e);
    }

    serializer = new KryoSerializer();
    serializer.register(Struct.class, new StructSerializer());

    LOG.info("FileTtlCacheProcessor opened({})", this);
  }

  @Override
  public void put(Object key, List<Object> values) throws ProcessorException {
    if (table == null) {
      throw new ProcessorException("Processor isn't open");
    }

    try {
      table.put(key, GungnirUtils.currentTimeSecs(), serializer.serialize(values));

      table.compactRange();
    } catch (RocksDBException e) {
      throw new ProcessorException(e);
    }
  }

  @Override
  public int size(Object key) {
    try {
      return table.size(key);
    } catch (RocksDBException e) {
      LOG.error("Failed to get size", e);
      return 0;
    }
  }

  @Override
  public List<List<Object>> take(Object key) {
    int now = GungnirUtils.currentTimeSecs();
    List<List<Object>> valuesList = Lists.newArrayList();
    EntryIterator it = null;
    try {
      for (it = table.iterator(key); it.hasNext();) {
        RocksDBTable.Entry entry = it.next();
        if (entry.getTimeKey() >= now) {
          @SuppressWarnings("unchecked")
          List<Object> values = serializer.deserialize(entry.getValue(), ArrayList.class);
          valuesList.add(values);
        }
        it.remove();
      }
    } catch (RocksDBException e) {
      LOG.error("Failed to take tuples", e);
    } finally {
      if (it != null) {
        try {
          it.close();
        } catch (RocksDBException e) {
          LOG.error("Failed to close iterator", e);
        }
      }
    }

    return valuesList;
  }

  @Override
  public void close() {
    if (table != null) {
      try {
        table.close();
      } catch (RocksDBException e) {
        LOG.error("Failed to close table", e);
      }
    }

    LOG.info("FileTtlCacheProcessor closed({})", this);
  }

  @Override
  public FileTtlCacheProcessor clone() {
    return new FileTtlCacheProcessor();
  }

  @Override
  public String toString() {
    return "file_cache()";
  }
}
