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
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class InMemoryEmitter extends BasePersistentEmitter {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryEmitter.class);

  private static final Map<String, LinkedBlockingQueue<List<Object>>> QUEUES_MAPS =
      Maps.newConcurrentMap();
  private static final int QUEUE_SIZE = 1024;

  @Override
  protected void prepare() {
  }

  @Override
  protected void sync() {
  }

  public static synchronized LinkedBlockingQueue<List<Object>> getQueue(String accountId,
      String tupleName) {
    LinkedBlockingQueue<List<Object>> queue = QUEUES_MAPS.get(accountId + "." + tupleName);
    if (queue == null) {
      queue = new LinkedBlockingQueue<List<Object>>(QUEUE_SIZE);
      QUEUES_MAPS.put(accountId + "." + tupleName, queue);
    }

    return queue;
  }

  @Override
  protected void emit(String accountId, List<TupleValues> tuples) {
    try {
      for (TupleValues tuple : tuples) {
        LinkedBlockingQueue<List<Object>> queue =
            QUEUES_MAPS.get(accountId + "." + tuple.getTupleName());
        if (queue != null) {
          queue.put(tuple.getValues());

          if (LOG.isDebugEnabled()) {
            LOG.debug("Insert tracking data. tuple: {}", tuple);
          }
        } else {
          LOG.warn("In-memory queue isn't ready");
        }
      }
    } catch (InterruptedException e) {
      LOG.info("In-memory queue interrupted");
    }
  }

  @Override
  public void cleanup() {
  }

  @Override
  public PersistentEmitter clone() {
    return new InMemoryEmitter();
  }
}
