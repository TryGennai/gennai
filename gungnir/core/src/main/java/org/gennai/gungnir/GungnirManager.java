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

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.concurrent.locks.ReentrantLock;

import org.gennai.gungnir.cluster.ClusterManager;
import org.gennai.gungnir.cluster.DistributedClusterManager;
import org.gennai.gungnir.cluster.LocalClusterManager;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.metrics.MetricsManager;
import org.gennai.gungnir.tuple.persistent.PersistentDeserializer;
import org.gennai.gungnir.tuple.persistent.PersistentDispatcher;
import org.gennai.gungnir.tuple.persistent.PersistentEmitter;
import org.gennai.gungnir.tuple.persistent.TrackingData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GungnirManager {

  private static final Logger LOG = LoggerFactory.getLogger(GungnirManager.class);

  private static volatile GungnirManager MANAGER;

  private GungnirConfig config;
  private volatile MetaStore metaStore;
  private ReentrantLock metaStoreLock;
  private volatile MetricsManager metricsManager;
  private ReentrantLock metricsManagerLock;
  private volatile ClusterManager clusterManager;
  private ReentrantLock clusterManagerLock;

  private GungnirManager(GungnirConfig config) {
    this.config = config;
    metaStoreLock = new ReentrantLock();
    metricsManagerLock = new ReentrantLock();
    clusterManagerLock = new ReentrantLock();
  }

  public static GungnirManager getManager() {
    if (MANAGER == null) {
      synchronized (GungnirManager.class) {
        if (MANAGER == null) {
          MANAGER = new GungnirManager(GungnirConfig.readGugnirConfig());
        }
      }
    }
    return MANAGER;
  }

  public GungnirConfig getConfig() {
    return config;
  }

  public MetaStore getMetaStore() throws MetaStoreException {
    if (metaStore == null) {
      metaStoreLock.lock();
      try {
        if (metaStore == null) {
          try {
            Class<?> metaStoreClass = config.getClass(METASTORE);
            if (MetaStore.class.isAssignableFrom(metaStoreClass)) {
              metaStore = (MetaStore) metaStoreClass.newInstance();
              metaStore.open();
            } else {
              LOG.error("Invalid metastore class '{}'", metaStoreClass.getName());
              throw new MetaStoreException("Failed to create metastore");
            }
          } catch (ClassNotFoundException e) {
            LOG.error("Failed to create metastore", e);
            throw new MetaStoreException("Failed to create metastore");
          } catch (InstantiationException e) {
            LOG.error("Failed to create metastore", e);
            throw new MetaStoreException("Failed to create metastore");
          } catch (IllegalAccessException e) {
            LOG.error("Failed to create metastore", e);
            throw new MetaStoreException("Failed to create metastore");
          }
        }
      } finally {
        metaStoreLock.unlock();
      }
    }
    return metaStore;
  }

  public MetricsManager getMetricsManager() {
    if (metricsManager == null) {
      metricsManagerLock.lock();
      try {
        if (metricsManager == null) {
          metricsManager = new MetricsManager(config);
        }
      } finally {
        metricsManagerLock.unlock();
      }
    }
    return metricsManager;
  }

  public PersistentDispatcher createDispatcher(UserEntity owner) throws MetaStoreException {
    PersistentDeserializer deserializer = null;
    try {
      Class<?> deserClass = config.getClass(PERSISTENT_DESERIALIZER);
      if (PersistentDeserializer.class.isAssignableFrom(deserClass)) {
        deserializer = (PersistentDeserializer) deserClass.newInstance();
      } else {
        LOG.error("Invalid persistent deserializer class '{}'", deserClass.getName());
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to create persistent deserializer", e);
    } catch (InstantiationException e) {
      LOG.error("Failed to create persistent deserializer", e);
    } catch (IllegalAccessException e) {
      LOG.error("Failed to create persistent deserializer", e);
    }

    PersistentEmitter emitter = null;
    try {
      Class<?> emitterClass = config.getClass(PERSISTENT_EMITTER);
      if (PersistentEmitter.class.isAssignableFrom(emitterClass)) {
        emitter = (PersistentEmitter) emitterClass.newInstance();
      } else {
        LOG.error("Invalid persistent emitter class '{}'", emitterClass.getName());
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to create persistent emitter", e);
    } catch (InstantiationException e) {
      LOG.error("Failed to create persistent emitter", e);
    } catch (IllegalAccessException e) {
      LOG.error("Failed to create persistent emitter", e);
    }

    if (deserializer != null && emitter != null) {
      return new PersistentDispatcher(owner, deserializer, emitter);
    } else {
      return null;
    }
  }

  public ClusterManager getClusterManager() {
    if (clusterManager == null) {
      clusterManagerLock.lock();
      try {
        if (clusterManager == null) {
          String clusterMode = config.getString(CLUSTER_MODE);
          if (clusterMode.equals(LOCAL_CLUSTER)) {
            clusterManager = new LocalClusterManager();
          } else {
            clusterManager = new DistributedClusterManager();
          }
        }
      } finally {
        clusterManagerLock.unlock();
      }
    }
    return clusterManager;
  }

  // TODO: No15
  public void dispatchTrackingData(String accountId, TrackingData trackingData)
      throws MetaStoreException, NotStoredException {
    PersistentDispatcher dispatcher = getClusterManager().getDispatcher(accountId);
    if (dispatcher != null) {
      dispatcher.dispatch(trackingData);
      if (LOG.isInfoEnabled()) {
        LOG.info("Accept tracking data {}", trackingData);
      }
    } else {
      LOG.warn("Invalid account ID '{}'", accountId);
    }
  }

  public void close() {
    if (metaStore != null) {
      metaStore.close();
    }

    if (metricsManager != null) {
      metricsManager.close();
    }

    if (clusterManager != null) {
      clusterManager.close();
    }
  }
}
