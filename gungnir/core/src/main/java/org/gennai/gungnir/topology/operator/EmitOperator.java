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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.topology.processor.EmitProcessor;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@Operator.Description(name = "EMIT", parameterNames = {"processor", "outputFields"})
public class EmitOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(EmitOperator.class);

  private class Emitter implements Runnable {

    @Override
    public void run() {
      LOG.info("Emitter started. ({} {})", getContext().getTopologyId(), getName());

      Map<String, List<String>> outputFieldNames = getContext().getOutputFields().get(getName());
      for (Map.Entry<String, List<String>> entry : outputFieldNames.entrySet()) {
        List<String> fieldNames = Lists.newArrayList();
        for (FieldAccessor field : outputFields) {
          if (field.getTupleAccessor() == null) {
            if (field.isWildcardField()) {
              fieldNames.addAll(entry.getValue());
            } else {
              fieldNames.add(field.getFieldName());
            }
          } else {
            if (field.getTupleAccessor().getTupleName().equals(entry.getKey())) {
              if (field.isWildcardField()) {
                fieldNames.addAll(entry.getValue());
              } else {
                fieldNames.add(field.getFieldName());
              }
            }
          }
        }
        outputFieldNames.put(entry.getKey(), fieldNames);
      }

      try {
        processor.open(getConfig(), getContext(), getOperatorContext(), outputFieldNames);

        List<TupleValues> tuples = Lists.newArrayList();
        while (!Thread.interrupted()) {
          TupleValues tupleValues = emitQueue.take();
          while (tupleValues != null) {
            tuples.add(tupleValues);
            if (tuples.size() >= emitMax) {
              break;
            }
            tupleValues = emitQueue.poll();
          }

          try {
            processor.write(tuples);
          } catch (ProcessorException e) {
            LOG.error("Failed to execute in processor", e);
          }
          tuples.clear();
        }
      } catch (ProcessorException e) {
        LOG.error("Failed to execute processor", e);
      } catch (InterruptedException e) {
        LOG.info("Emitter interrupted");
      } finally {
        if (!emitQueue.isEmpty()) {
          try {
            List<TupleValues> tuples = Lists.newArrayList();
            for (Iterator<TupleValues> it = emitQueue.iterator(); !Thread.interrupted()
                && it.hasNext();) {
              TupleValues tupleValues = it.next();
              tuples.add(tupleValues);

              if (tuples.size() >= emitMax) {
                processor.write(tuples);
                tuples = Lists.newArrayList();
              }
            }

            if (!tuples.isEmpty()) {
              processor.write(tuples);
            }
          } catch (ProcessorException e) {
            LOG.error("Failed to execute processor", e);
          }
        }

        processor.close();
      }
    }
  }

  private EmitProcessor processor;
  private FieldAccessor[] outputFields;
  private Emitter emitter;
  private ExecutorService emitExecutor;
  private LinkedBlockingQueue<TupleValues> emitQueue;
  private int emitMax;

  public EmitOperator(EmitProcessor processor, FieldAccessor[] outputFields) {
    super();
    this.processor = processor;
    this.outputFields = outputFields;
  }

  private EmitOperator(EmitOperator c) {
    super(c);
    this.processor = c.processor;
    this.outputFields = c.outputFields;
    this.emitter = c.emitter;
    this.emitExecutor = c.emitExecutor;
    this.emitQueue = c.emitQueue;
    this.emitMax = c.emitMax;
  }

  @Override
  protected void prepare() {
    if (emitter == null) {
      emitter = new Emitter();
      emitExecutor = Executors.newSingleThreadExecutor(GungnirUtils.createThreadFactory(getName()));
      emitExecutor.execute(emitter);
      Integer queueSize = getConfig().getInteger(EMIT_OPERATOR_QUEUE_SIZE + "." + getId());
      if (queueSize == null) {
        queueSize = getConfig().getInteger(EMIT_OPERATOR_QUEUE_SIZE);
      }
      emitQueue = new LinkedBlockingQueue<TupleValues>(queueSize);
      Integer max = getConfig().getInteger(EMIT_OPERATOR_EMIT_TUPLES_MAX + "." + getId());
      if (max == null) {
        max = getConfig().getInteger(EMIT_OPERATOR_EMIT_TUPLES_MAX);
      }
      emitMax = max;
    }
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    List<Object> values = Lists.newArrayList();
    for (FieldAccessor field : outputFields) {
      if (field.isWildcardField()) {
        if (((FieldAccessor) field).getTupleAccessor() == null) {
          values.addAll(tuple.getTupleValues().getValues());
        } else {
          if (tuple.getTupleName().equals(
              ((FieldAccessor) field).getTupleAccessor().getTupleName())) {
            values.addAll(tuple.getTupleValues().getValues());
          }
        }
      } else {
        values.add(field.getValue(tuple));
      }
    }

    try {
      emitQueue.put(new TupleValues(tuple.getTupleName(), values));
    } catch (InterruptedException e) {
      LOG.info("Emit queue interrupted");
    }

    dispatch(tuple.getTupleValues());
  }

  @Override
  protected void cleanup() {
    // TODO Test called cleanup
    emitExecutor.shutdownNow();
    LOG.info("Emit executor shutdown");
    try {
      long timeout = emitQueue.size() * 100 + TERMINATION_WAIT_TIME;
      if (!emitExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
        emitExecutor.shutdownNow();
        LOG.info("Emit executor forced shutdown");
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to shutdown emit executor", e);
    }
  }

  @Override
  public EmitOperator clone() {
    return new EmitOperator(this);
  }
}
