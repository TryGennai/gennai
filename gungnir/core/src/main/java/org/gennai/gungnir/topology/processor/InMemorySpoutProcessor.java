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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.processor.spout.MessageId;
import org.gennai.gungnir.topology.processor.spout.TupleAndMessageId;
import org.gennai.gungnir.tuple.persistent.InMemoryEmitter;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.gennai.gungnir.tuple.schema.ViewSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class InMemorySpoutProcessor implements SpoutProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(InMemorySpoutProcessor.class);
  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String schemaName;
  private LinkedBlockingQueue<List<Object>> queue;

  @Override
  public void open(GungnirConfig config, GungnirContext context, Schema schema)
      throws ProcessorException {
    if (schema instanceof TupleSchema) {
      schemaName = schema.getSchemaName();
    } else if (schema instanceof ViewSchema) {
      schemaName = ((ViewSchema) schema).getTupleSchema().getSchemaName();
    }
    queue = InMemoryEmitter.getQueue(context.getAccountId(), schemaName);
    LOG.info("InMemorySpoutProcessor opened({})", schemaName);
  }

  @Override
  public List<TupleAndMessageId> read() throws ProcessorException, InterruptedException {
    List<TupleAndMessageId> tuples = Lists.newArrayList();
    List<Object> values = queue.take();
    while (values != null) {
      tuples.add(new TupleAndMessageId(values, null));
      values = queue.poll();
    }
    return tuples;
  }

  @Override
  public void ack(MessageId messageId) {
  }

  @Override
  public void fail(MessageId messageId) {
  }

  @Override
  public void close() {
    LOG.info("InMemorySpoutProcessor closed({})", schemaName);
  }

  @Override
  public InMemorySpoutProcessor clone() {
    return new InMemorySpoutProcessor();
  }

  @Override
  public String toString() {
    return "memory_spout()";
  }
}
