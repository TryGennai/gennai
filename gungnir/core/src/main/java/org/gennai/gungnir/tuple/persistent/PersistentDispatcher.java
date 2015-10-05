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
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Lists;

public class PersistentDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(PersistentDispatcher.class);

  private UserEntity owner;
  private PersistentDeserializer persistentDeserializer;
  private PersistentEmitter persistentEmitter;
  private GungnirConfig config;
  private MetaStore metaStore;
  private ReentrantReadWriteLock syncLock;
  private SchemaRegistry schemaRegistry;
  private LinkedBlockingQueue<TrackingData> deserQueue;
  private List<PersistentDeserializer> deserializers;
  private ExecutorService deserExecutor;
  private LinkedBlockingQueue<TupleValues> emitQueue;
  private List<PersistentEmitter> emitters;
  private ExecutorService emitExecutor;
  private Metrics metrics;

  class Metrics {

    private MetricRegistry metricRegistry;
    private Timer dispatcheTimer;
    private Timer deserTimer;
    private Timer emitTimer;
    private Histogram deserSize;
    private Histogram emitSize;
    private Meter emitCount;

    void prepare() {
      metricRegistry = GungnirManager.getManager().getMetricsManager().getRegistry();
      metricRegistry.register(METRICS_PERSISTENT_DESER_QUEUE_SIZE + "." + owner.getId(),
          new Gauge<Integer>() {

            @Override
            public Integer getValue() {
              return deserQueue.size();
            }
          });
      metricRegistry.register(METRICS_PERSISTENT_EMITTER_QUEUE_SIZE + "." + owner.getId(),
          new Gauge<Integer>() {

            @Override
            public Integer getValue() {
              return emitQueue.size();
            }
          });

      dispatcheTimer = metricRegistry.timer(METRICS_PERSISTENT_DISPATCH_TIME + "." + owner.getId());
      deserTimer = metricRegistry.timer(METRICS_PERSISTENT_DESER_TIME + "." + owner.getId());
      emitTimer = metricRegistry.timer(METRICS_PERSISTENT_EMIT_TIME + "." + owner.getId());

      deserSize = metricRegistry.histogram(METRICS_PERSISTENT_DESER_SIZE + "." + owner.getId());
      emitSize = metricRegistry.histogram(METRICS_PERSISTENT_EMIT_SIZE + "." + owner.getId());

      emitCount = metricRegistry.meter(METRICS_PERSISTENT_EMIT_COUNT + "." + owner.getId());
    }

    Timer getDispatcheTimer() {
      return dispatcheTimer;
    }

    Timer getDeserTimer() {
      return deserTimer;
    }

    Timer getEmitTimer() {
      return emitTimer;
    }

    Histogram getDeserSize() {
      return deserSize;
    }

    Histogram getEmitSize() {
      return emitSize;
    }

    Meter getEmitCount() {
      return emitCount;
    }

    void cleanup() {
      metricRegistry.remove(METRICS_PERSISTENT_DESER_QUEUE_SIZE + "." + owner.getId());
      metricRegistry.remove(METRICS_PERSISTENT_EMITTER_QUEUE_SIZE + "." + owner.getId());

      metricRegistry.remove(METRICS_PERSISTENT_DISPATCH_TIME + "." + owner.getId());
      metricRegistry.remove(METRICS_PERSISTENT_DESER_TIME + "." + owner.getId());
      metricRegistry.remove(METRICS_PERSISTENT_EMIT_TIME + "." + owner.getId());

      metricRegistry.remove(METRICS_PERSISTENT_EMIT_SIZE + "." + owner.getId());
      metricRegistry.remove(METRICS_PERSISTENT_DESER_SIZE + "." + owner.getId());
      metricRegistry.remove(METRICS_PERSISTENT_EMIT_COUNT + "." + owner.getId());
    }
  }

  public PersistentDispatcher(UserEntity owner, PersistentDeserializer persistentDeserializer,
      PersistentEmitter persistentEmitter) throws MetaStoreException {
    this.owner = owner;
    this.persistentDeserializer = persistentDeserializer;
    this.persistentEmitter = persistentEmitter;

    config = GungnirManager.getManager().getConfig();

    deserQueue = new LinkedBlockingQueue<TrackingData>(
        config.getInteger(PERSISTENT_DESER_QUEUE_SIZE));
    emitQueue = new LinkedBlockingQueue<TupleValues>(
        config.getInteger(PERSISTENT_EMITTER_QUEUE_SIZE));

    metrics = new Metrics();
    metrics.prepare();

    persistentDeserializer.prepare(this);

    persistentEmitter.prepare(this);

    metaStore = GungnirManager.getManager().getMetaStore();
    syncLock = new ReentrantReadWriteLock();
  }

  public UserEntity getOwner() {
    return owner;
  }

  public GungnirConfig getConfig() {
    return config;
  }

  public SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  LinkedBlockingQueue<TrackingData> getDeserQueue() {
    return deserQueue;
  }

  LinkedBlockingQueue<TupleValues> getEmitQueue() {
    return emitQueue;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void sync(SchemaRegistry schemaRegistry) throws MetaStoreException {
    WriteLock writeLock = syncLock.writeLock();
    writeLock.lock();

    try {
      if (this.schemaRegistry == null) {
        int deserParallelism = config.getInteger(PERSISTENT_DESER_PARALLELISM);
        deserExecutor = Executors.newFixedThreadPool(deserParallelism,
            GungnirUtils.createThreadFactory("PersistentDeserializer"));
        deserializers = Lists.newArrayListWithCapacity(deserParallelism);

        for (int i = 0; i < deserParallelism; i++) {
          PersistentDeserializer deserializer = persistentDeserializer.clone();
          deserializer.prepare(this);
          deserializer.sync(schemaRegistry);
          deserExecutor.execute(deserializer);
          deserializers.add(deserializer);
        }

        int emitterParallelism = config.getInteger(PERSISTENT_EMITTER_PARALLELISM);
        emitExecutor = Executors.newFixedThreadPool(emitterParallelism,
            GungnirUtils.createThreadFactory("PersistentEmitter"));
        emitters = Lists.newArrayListWithCapacity(emitterParallelism);

        for (int i = 0; i < emitterParallelism; i++) {
          PersistentEmitter emitter = persistentEmitter.clone();
          emitter.prepare(this);
          emitter.sync(schemaRegistry);
          emitExecutor.execute(emitter);
          emitters.add(emitter);
        }

        this.schemaRegistry = schemaRegistry;
      } else {
        for (PersistentDeserializer deserializer : deserializers) {
          deserializer.pause();
        }

        for (PersistentDeserializer deserializer : deserializers) {
          deserializer.sync(schemaRegistry);
        }

        for (PersistentEmitter emitter : emitters) {
          emitter.sync(schemaRegistry);
        }

        for (PersistentDeserializer deserializer : deserializers) {
          deserializer.resume();
        }

        this.schemaRegistry = schemaRegistry;
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void dispatch(TrackingData trackingData) throws MetaStoreException, NotStoredException {
    Context timerContext = metrics.getDispatcheTimer().time();

    ReadLock readLock = syncLock.readLock();
    readLock.lock();

    try {
      Schema schema = schemaRegistry.get(trackingData.getTupleName());
      if (schema != null && schema instanceof TupleSchema) {
        if (schema.getFieldIndex(TRACKING_ID_FIELD) != null
            || schema.getFieldIndex(TRACKING_NO_FIELD) != null) {
          if (trackingData.getTid() == null) {
            trackingData.setTid(metaStore.generateTrackingId());
            trackingData.setTno(metaStore.getTrackingNo(trackingData.getTid()));
          } else {
            trackingData.setTno(metaStore.getTrackingNo(trackingData.getTid()));
          }
        }
        deserQueue.put(trackingData);
      } else {
        LOG.info("{} has not been accepted", trackingData.getTupleName());
      }
    } catch (InterruptedException e) {
      LOG.info("Deserialize queue interrupted");
    } finally {
      readLock.unlock();
      timerContext.stop();
    }
  }

  public void close() {
    WriteLock writeLock = syncLock.writeLock();
    writeLock.lock();

    try {
      deserExecutor.shutdownNow();
      LOG.info("Deserializer shutdown. account ID: '{}'", owner.getId());
      try {
        long timeout = deserQueue.size() * 100 + TERMINATION_WAIT_TIME;
        if (!deserExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
          deserExecutor.shutdownNow();
          LOG.warn("Deserializer forced shutdown. account ID: '{}'", owner.getId());
        }
      } catch (InterruptedException e) {
        LOG.error("Failed to shutdown deserializer", e);
      }

      emitExecutor.shutdownNow();
      LOG.info("Emitter shutdown. account ID: '{}'", owner.getId());
      try {
        long timeout = emitQueue.size() * 100 + TERMINATION_WAIT_TIME;
        if (!emitExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
          emitExecutor.shutdownNow();
          LOG.warn("Emitter forced shutdown. account ID: '{}'", owner.getId());
        }
      } catch (InterruptedException e) {
        LOG.error("Failed to shutdown emitter", e);
      }
    } finally {
      writeLock.unlock();
    }

    persistentEmitter.cleanup();

    metrics.cleanup();
  }
}
