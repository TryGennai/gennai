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

package org.gennai.gungnir.cluster;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.ql.session.LocalSessionStore;
import org.gennai.gungnir.ql.session.SessionStore;
import org.gennai.gungnir.ql.session.SessionStoreException;
import org.gennai.gungnir.tuple.persistent.PersistentDispatcher;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class LocalClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(LocalClusterManager.class);

  private Map<String, PersistentDispatcher> dispatchersMap = Maps.newConcurrentMap();
  private Map<String, UserEntity> usersByName = Maps.newConcurrentMap();
  private volatile SessionStore sessionStore;
  private ReentrantLock sessionStoreLock = new ReentrantLock();

  @Override
  public void start() throws ClusterManagerException {
    LOG.info("Local cluster manager started");
  }

  @Override
  public void join() throws ClusterManagerException {
    LOG.info("Join cluster");
  }

  @Override
  public PersistentDispatcher getDispatcher(String accountId) {
    return dispatchersMap.get(accountId);
  }

  @Override
  public UserEntity getUserByName(String userName) {
    return usersByName.get(userName);
  }

  @Override
  public synchronized void sync(GungnirTopology topology) throws ClusterManagerException {
    try {
      GungnirManager manager = GungnirManager.getManager();
      MetaStore metaStore = manager.getMetaStore();

      PersistentDispatcher dispatcher = dispatchersMap.get(topology.getOwner().getId());
      if (dispatcher == null) {
        if (topology.getStatus() == TopologyStatus.RUNNING) {
          SchemaRegistry schemaRegistry = new SchemaRegistry();
          schemaRegistry.registerAll(topology.getUsedSchemas());

          dispatcher = manager.createDispatcher(topology.getOwner());
          if (dispatcher != null) {
            dispatcher.sync(schemaRegistry);
            dispatchersMap.put(topology.getOwner().getId(), dispatcher);
            usersByName.put(topology.getOwner().getName(), topology.getOwner());

            LOG.info(
                "Synchronized the cluster. account: {}, topology: {} {}, accepting tuples: {}",
                topology.getOwner().getId(), topology.getId(), topology.getStatus(),
                schemaRegistry);
          }
        }
      } else {
        SchemaRegistry schemaRegistry = dispatcher.getSchemaRegistry();
        SchemaRegistry registryCopy = schemaRegistry.clone();

        boolean changed = false;
        for (Schema schema : topology.getUsedSchemas()) {
          if (topology.getStatus() == TopologyStatus.RUNNING) {
            if (!registryCopy.exists(schema.getSchemaName())) {
              registryCopy.register(schema);
              changed = true;
            }
          } else {
            try {
              Schema storedSchema =
                  metaStore.findSchema(schema.getSchemaName(), topology.getOwner());
              storedSchema.getTopologies().remove(topology.getId());
              if (storedSchema.getTopologies().isEmpty()) {
                registryCopy.unregister(schema.getSchemaName());
                changed = true;
              }
            } catch (NotStoredException e) {
              registryCopy.unregister(schema.getSchemaName());
              changed = true;
            }
          }
        }

        if (changed) {
          if (registryCopy.getSchemas().isEmpty()) {
            dispatcher.close();
            dispatchersMap.remove(topology.getOwner().getId());
            usersByName.remove(topology.getOwner().getName());
          } else {
            dispatcher.sync(registryCopy);
          }

          LOG.info(
              "Synchronized the cluster. account: {}, topology: {} ({}), accepting tuples: {}",
              topology.getOwner().getId(), topology.getId(), topology.getStatus(), registryCopy);
        }
      }
    } catch (MetaStoreException e) {
      throw new ClusterManagerException(e);
    }
  }

  @Override
  public SessionStore getSessionStore() throws SessionStoreException {
    if (sessionStore == null) {
      sessionStoreLock.lock();
      try {
        if (sessionStore == null) {
          sessionStore = new LocalSessionStore();
          sessionStore.open();
        }
      } finally {
        sessionStoreLock.unlock();
      }
    }
    return sessionStore;
  }

  @Override
  public void close() {
    for (Map.Entry<String, PersistentDispatcher> entry : dispatchersMap.entrySet()) {
      PersistentDispatcher dispatcher = entry.getValue();
      dispatcher.close();
      LOG.info("Persistent dispatcher closed. account: '{}'", entry.getKey());
    }

    if (sessionStore != null) {
      try {
        sessionStore.close();
      } catch (SessionStoreException e) {
        LOG.error("Failed to close session store");
      }
    }
  }
}
