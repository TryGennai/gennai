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

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Lists;

public abstract class BasePersistentEmitter implements PersistentEmitter {

  private static final Logger LOG = LoggerFactory.getLogger(PersistentEmitter.class);

  private PersistentDispatcher dispatcher;
  private SchemaRegistry schemaRegistry;
  private ReentrantLock stateLock = new ReentrantLock();
  private Condition pauseCond = stateLock.newCondition();
  private Condition resumeCond = stateLock.newCondition();
  private volatile boolean paused;
  private volatile boolean resumed;

  protected BasePersistentEmitter() {
  }

  protected BasePersistentEmitter(BasePersistentEmitter c) {
    this.dispatcher = c.dispatcher;
  }

  protected PersistentDispatcher getDispatcher() {
    return dispatcher;
  }

  protected SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
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
      LOG.info("Persistent emitter paused. account ID: '{}'", dispatcher.getOwner().getId());

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
      LOG.info("Persistent emitter resume. account ID: '{}'", dispatcher.getOwner().getId());
    }
  }

  protected abstract void emit(String accountId, List<TupleValues> tuples);

  @Override
  public void run() {
    LOG.info("Persistent emitter started. account ID: '{}'", dispatcher.getOwner().getId());

    int max = dispatcher.getConfig().getInteger(PERSISTENT_EMIT_TUPLES_MAX);

    try {
      while (!Thread.interrupted()) {
        List<TupleValues> tuples = Lists.newArrayList();

        TupleValues tupleValues = dispatcher.getEmitQueue().take();
        while (tupleValues != null) {
          tuples.add(tupleValues);
          if (tuples.size() >= max) {
            break;
          }
          tupleValues = dispatcher.getEmitQueue().poll();
        }

        Context timerContext = dispatcher.getMetrics().getEmitTimer().time();
        try {
          await();
          emit(dispatcher.getOwner().getId(), tuples);
        } finally {
          timerContext.stop();
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Persistent emitter interrupted");
    }

    if (!dispatcher.getEmitQueue().isEmpty()) {
      List<TupleValues> tuples = Lists.newArrayList();
      for (Iterator<TupleValues> it = dispatcher.getEmitQueue().iterator(); !Thread.interrupted()
          && it.hasNext();) {
        TupleValues tupleValues = it.next();
        tuples.add(tupleValues);

        if (tuples.size() >= max) {
          emit(dispatcher.getOwner().getId(), tuples);
          tuples = Lists.newArrayList();
        }
      }

      if (!tuples.isEmpty()) {
        emit(dispatcher.getOwner().getId(), tuples);
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
      LOG.info("Persistent emitter interrupted");
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
  public abstract PersistentEmitter clone();
}
