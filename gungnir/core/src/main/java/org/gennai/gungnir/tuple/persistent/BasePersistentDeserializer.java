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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;

public abstract class BasePersistentDeserializer implements PersistentDeserializer {

  private static final Logger LOG = LoggerFactory.getLogger(BasePersistentDeserializer.class);

  private PersistentDispatcher dispatcher;
  private SchemaRegistry schemaRegistry;
  private ReentrantLock stateLock = new ReentrantLock();
  private Condition pauseCond = stateLock.newCondition();
  private Condition resumeCond = stateLock.newCondition();
  private volatile boolean paused;
  private boolean resumed;

  protected BasePersistentDeserializer() {
  }

  protected BasePersistentDeserializer(BasePersistentDeserializer c) {
    this.dispatcher = c.dispatcher;
  }

  protected PersistentDispatcher getDispatcher() {
    return dispatcher;
  }

  protected abstract void prepare();

  @Override
  public void prepare(PersistentDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    prepare();
  }

  protected abstract void sync();

  @Override
  public void sync(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
    sync();
  }

  private void await() throws InterruptedException {
    if (paused) {
      LOG.info("Persistent deserializer paused. account ID: '{}'", dispatcher.getOwner().getId());

      stateLock.lock();
      try {
        paused = false;
        resumed = false;
        pauseCond.signal();
        while (!resumed) {
          resumeCond.await();
        }
      } finally {
        stateLock.unlock();
      }
      LOG.info("Persistent deserializer resume. account ID: '{}'", dispatcher.getOwner().getId());
    }
  }

  protected abstract TupleValues deserialize(TrackingData trackingData, Schema schema)
      throws DeserializeException;

  private TupleValues doDeserialize(TrackingData trackingData) throws DeserializeException {
    getDispatcher().getMetrics().getDeserSize()
        .update(trackingData.getContent().toString().length());

    Context timerContext = dispatcher.getMetrics().getDeserTimer().time();

    Schema schema = schemaRegistry.get(trackingData.getTupleName());
    if (schema == null || !(schema instanceof TupleSchema)) {
      LOG.warn("Tuple {} isn't registered", trackingData.getTupleName());
      throw new DeserializeException(
          "Tuple " + trackingData.getTupleName() + " isn't registered");
    }

    TupleValues tupleValues = null;
    try {
      tupleValues = deserialize(trackingData, schema);

      Integer index = schema.getFieldIndex(TRACKING_ID_FIELD);
      if (index != null) {
        tupleValues.getValues().set(index, trackingData.getTid());
      }
      index = schema.getFieldIndex(TRACKING_NO_FIELD);
      if (index != null) {
        tupleValues.getValues().set(index, trackingData.getTno());
      }
      index = schema.getFieldIndex(ACCEPT_TIME_FIELD);
      if (index != null) {
        tupleValues.getValues().set(index, new Date());
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Deserialized tracking data {} to tuple values {}", trackingData, tupleValues);
      }
    } finally {
      timerContext.stop();
    }

    return tupleValues;
  }

  @Override
  public void run() {
    LOG.info("Persistent deserializer started. account ID: '{}'", dispatcher.getOwner().getId());

    try {
      while (!Thread.interrupted()) {
        TrackingData trackingData = dispatcher.getDeserQueue().take();
        try {
          while (trackingData != null) {
            await();
            dispatcher.getEmitQueue().put(doDeserialize(trackingData));
            trackingData = dispatcher.getDeserQueue().poll();
          }
        } catch (DeserializeException e) {
          LOG.warn("Failed to deserialize", e);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Persistent deserializer interrupted");
    }

    if (!dispatcher.getDeserQueue().isEmpty()) {
      for (Iterator<TrackingData> it = dispatcher.getDeserQueue().iterator(); !Thread.interrupted()
          && it.hasNext();) {
        TrackingData trackingData = it.next();
        try {
          dispatcher.getEmitQueue().put(doDeserialize(trackingData));
        } catch (InterruptedException e) {
          LOG.info("Persistent emit queue interrupted");
        } catch (DeserializeException e) {
          LOG.warn("Failed to deserialize", e);
        }
      }
    }
  }

  @Override
  public void pause() {
    stateLock.lock();
    try {
      if (!paused && stateLock.getWaitQueueLength(resumeCond) == 0) {
        paused = true;
        if (!dispatcher.getDeserQueue().isEmpty()) {
          while (paused) {
            pauseCond.await();
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Persistent deserializer interrupted");
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public void resume() {
    stateLock.lock();
    try {
      paused = false;
      resumed = true;
      resumeCond.signal();
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public abstract PersistentDeserializer clone();
}
