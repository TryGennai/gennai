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

package org.gennai.gungnir;

import static org.gennai.gungnir.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmitMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(EmitMonitor.class);

  private CountDownLatch replied;
  private Map<String, List<Map<String, Object>>> emitTuplesMap;

  public void prepare(Map<String, List<Map<String, Object>>> emitTuplesMap) {
    this.emitTuplesMap = emitTuplesMap;
    if (emitTuplesMap.size() > 0) {
      int count = 0;
      for (List<Map<String, Object>> emitTuples : emitTuplesMap.values()) {
        count += emitTuples.size();
      }
      replied = new CountDownLatch(count);
    }
  }

  public void start(int timeout) {
    try {
      if (replied != null) {
        LOG.info("Monitoring start. emit tuples: {}", emitTuplesMap);
        if (!replied.await(timeout, TimeUnit.SECONDS)) {
          fail("Number of tuple that are emit doesn't match");
        }
      } else {
        LOG.info("Waiting {}sec", timeout);
        TimeUnit.SECONDS.sleep(timeout);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void emitted(String emitName, GungnirTuple tuple) {
    LOG.info("Emit tuple ({})", tuple);

    if (emitTuplesMap.size() > 0) {
      List<Map<String, Object>> emitTuples = emitTuplesMap.get(emitName);
      LOG.info("Compare tuple ({}) has ({})", emitTuples, tuple);
      assertThat(tuple, compareTuple(emitTuples));
      replied.countDown();
    }
  }
}
