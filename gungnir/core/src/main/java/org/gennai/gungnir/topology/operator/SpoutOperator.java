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

package org.gennai.gungnir.topology.operator;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.gennai.gungnir.topology.ConditionEvaluator;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.topology.processor.SpoutProcessor;
import org.gennai.gungnir.topology.processor.spout.TupleAndMessageId;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.gennai.gungnir.tuple.schema.ViewSchema;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Operator.Description(name = "SPOUT", parameterNames = {"processor", "schemas"})
public class SpoutOperator extends BaseOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(SpoutOperator.class);

  private class Reader implements Runnable {

    private Schema schema;

    Reader(Schema schema) {
      this.schema = schema;
    }

    @Override
    public void run() {
      LOG.info("Reader started. ({} {})", getContext().getTopologyId(), getName());

      SpoutProcessor processorCopy = null;
      try {
        processorCopy = processor.clone();
        processorCopy.open(getConfig(), getContext(), schema);
        while (!Thread.interrupted()) {
          List<TupleAndMessageId> tupleAndMessageIds = processorCopy.read();
          if (tupleAndMessageIds != null) {
            for (TupleAndMessageId tupleAndMessageId : tupleAndMessageIds) {
              TupleValues tupleValues = new TupleValues(schema.getSchemaName(),
                  tupleAndMessageId.getValues());

              if (schema instanceof TupleSchema) {
                spoutQueue.put(tupleValues);
              } else if (schema instanceof ViewSchema) {
                Condition condition = ((ViewSchema) schema).getCondition();
                GungnirTuple tuple = new GungnirTuple(
                    getContext().getOutputFields().get(getName()).get(tupleValues.getTupleName()),
                    tupleValues);
                if (ConditionEvaluator.isKeep(condition, tuple)) {
                  spoutQueue.put(tupleValues);
                }
              }

              // TODO ack
              processorCopy.ack(tupleAndMessageId.getMessageId());
            }
          }
        }
      } catch (ProcessorException e) {
        LOG.error("Failed to execute processor", e);
      } catch (InterruptedException e) {
        LOG.info("Reader interrupted");
      } finally {
        if (processorCopy != null) {
          processorCopy.close();
        }
      }
    }
  }

  private SpoutProcessor processor;
  private Schema[] schemas;
  private Reader[] readers;
  private ExecutorService spoutExecutor;
  private LinkedBlockingQueue<TupleValues> spoutQueue;

  public SpoutOperator(SpoutProcessor processor, Schema[] schemas) {
    super();
    this.processor = processor;
    this.schemas = schemas;
  }

  public Schema[] getSchemas() {
    return schemas;
  }

  @Override
  protected void prepare() {
    spoutQueue =
        new LinkedBlockingQueue<TupleValues>(getConfig().getInteger(SPOUT_OPERATOR_QUEUE_SIZE));

    spoutExecutor = Executors.newFixedThreadPool(schemas.length,
        GungnirUtils.createThreadFactory(getName()));
    readers = new Reader[schemas.length];
    for (int i = 0; i < schemas.length; i++) {
      readers[i] = new Reader(schemas[i]);
      spoutExecutor.execute(readers[i]);
    }
  }

  public void nextTuple() {
    try {
      TupleValues tupleValues = spoutQueue.take();
      while (tupleValues != null) {
        dispatch(tupleValues);
        tupleValues = spoutQueue.poll();
      }
    } catch (InterruptedException e) {
      LOG.info("nextTuple interrupted");
    }
  }

  @Override
  protected void cleanup() {
    spoutExecutor.shutdownNow();
    LOG.info("Spout executor shutdown");
  }
}
