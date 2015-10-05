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

package org.gennai.gungnir.tuple.persistent;

import java.util.List;

import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.serialization.StructSerializer;
import org.gennai.gungnir.utils.KryoSerializer;

public class BenchmarkPersistentEmitter extends BasePersistentEmitter {

  private KryoSerializer serializer;

  @Override
  protected void prepare() {
    serializer = new KryoSerializer();
    serializer.register(Struct.class, new StructSerializer());
  }

  @Override
  protected void sync() {
  }

  @Override
  protected void emit(String accountId, List<TupleValues> tuples) {
    for (TupleValues tupleValues : tuples) {
      byte[] bytes = serializer.serialize(tupleValues.getValues());
      getDispatcher().getMetrics().getEmitSize().update(bytes.length);
    }
    getDispatcher().getMetrics().getEmitCount().mark(tuples.size());
  }

  @Override
  public void cleanup() {
  }

  @Override
  public PersistentEmitter clone() {
    return new BenchmarkPersistentEmitter();
  }
}
