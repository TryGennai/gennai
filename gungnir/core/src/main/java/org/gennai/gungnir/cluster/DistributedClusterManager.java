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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.ql.session.DistributedSessionStore;
import org.gennai.gungnir.ql.session.SessionStore;
import org.gennai.gungnir.ql.session.SessionStoreException;
import org.gennai.gungnir.tuple.persistent.PersistentDispatcher;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DistributedClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedClusterManager.class);

  private GungnirManager manager;
  private MetaStore metaStore;
  private CuratorFramework curator;
  private String topologiesPath;
  private Set<String> curTopologyIds;
  private Map<String, PersistentDispatcher> dispatchersMap = Maps.newConcurrentMap();
  private Map<String, UserEntity> usersByName = Maps.newConcurrentMap();
  private volatile SessionStore sessionStore;
  private ReentrantLock sessionStoreLock = new ReentrantLock();

  @Override
  public void start() throws ClusterManagerException {
    try {
      manager = GungnirManager.getManager();
      metaStore = manager.getMetaStore();
      GungnirConfig config = manager.getConfig();

      List<String> zkServers = config.getList(CLUSTER_ZOOKEEPER_SERVERS);

      curator = CuratorFrameworkFactory.builder()
          .connectString(StringUtils.join(zkServers, ","))
          .sessionTimeoutMs(config.getInteger(CLUSTER_ZOOKEEPER_SESSION_TIMEOUT))
          .connectionTimeoutMs(config.getInteger(CLUSTER_ZOOKEEPER_CONNECTION_TIMEOUT))
          .retryPolicy(new RetryNTimes(config.getInteger(CLUSTER_ZOOKEEPER_RETRY_TIMES),
              config.getInteger(CLUSTER_ZOOKEEPER_RETRY_INTERVAL))).build();

      curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
          LOG.info("Connection state changed. state: {}", newState);
        }
      });

      curator.start();

      topologiesPath = config.getString(GUNGNIR_NODE_PATH) + TOPOLOGIES_NODE_PATH;
      if (curator.checkExists().forPath(topologiesPath) == null) {
        try {
          curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(topologiesPath);
        } catch (KeeperException.NodeExistsException ignore) {
          ignore = null;
        }
      }

      LOG.info("Distributed cluster manager started");
    } catch (Exception e) {
      throw new ClusterManagerException(e);
    }
  }

  private void removeTopology(String topologyId) throws MetaStoreException {
    try {
      GungnirTopology topology = metaStore.findTopologyById(topologyId);

      PersistentDispatcher dispatcher = dispatchersMap.get(topology.getOwner().getId());
      if (dispatcher != null) {
        if (topology.getStatus() != TopologyStatus.RUNNING) {
          SchemaRegistry schemaRegistry = dispatcher.getSchemaRegistry();
          SchemaRegistry registryCopy = schemaRegistry.clone();

          boolean changed = false;
          for (Schema schema : topology.getUsedSchemas()) {
            try {
              try {
                Schema storedSchema =
                    metaStore.findSchema(schema.getSchemaName(), topology.getOwner());
                storedSchema.getTopologies().remove(topologyId);
                if (storedSchema.getTopologies().isEmpty()) {
                  registryCopy.unregister(schema.getSchemaName());
                  changed = true;
                }
              } catch (MetaStoreException e) {
                registryCopy.unregister(schema.getSchemaName());
                changed = true;
              }
            } catch (NotStoredException e) {
              registryCopy.unregister(schema.getSchemaName());
              changed = true;
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
      }
    } catch (NotStoredException e) {
      LOG.warn("Target topology doesn't exist. topology: {}", topologyId, e);
    }
  }

  private void addTopology(String topologyId) throws MetaStoreException {
    try {
      GungnirTopology topology = metaStore.findTopologyById(topologyId);

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
                "Synchronized the cluster. account: {}, topology: {} ({}), accepting tuples: {}",
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
          }
        }

        if (changed) {
          dispatcher.sync(registryCopy);

          LOG.info(
              "Synchronized the cluster. account: {}, topology: {} ({}), accepting tuples: {}",
              topology.getOwner().getId(), topology.getId(), topology.getStatus(), registryCopy);
        }
      }
    } catch (NotStoredException e) {
      LOG.warn("Target topology doesn't exist. topology: {}", topologyId, e);
    }
  }

  private synchronized void sync() throws Exception {
    List<String> topologyIds = curator.getChildren().watched().forPath(topologiesPath);

    Set<String> newTopologyIds = Sets.newHashSet(topologyIds);
    newTopologyIds.removeAll(curTopologyIds);

    Set<String> delTopologyIds = Sets.newHashSet(curTopologyIds);
    delTopologyIds.removeAll(topologyIds);

    for (String topologyId : delTopologyIds) {
      removeTopology(topologyId);
      curTopologyIds.remove(topologyId);

    }

    for (String topologyId : newTopologyIds) {
      addTopology(topologyId);
      curTopologyIds.add(topologyId);
    }
  }

  @Override
  public void join() throws ClusterManagerException {
    try {
      curator.getCuratorListenable().addListener(new CuratorListener() {

        @Override
        public void eventReceived(CuratorFramework curator, CuratorEvent event) throws Exception {
          if (event.getType() == CuratorEventType.WATCHED
              && event.getWatchedEvent().getType() == EventType.NodeChildrenChanged
              && event.getWatchedEvent().getPath().equals(topologiesPath)) {
            sync();
          }
        }
      });

      curTopologyIds = Sets.newHashSet();
      List<String> topologyIds = curator.getChildren().watched().forPath(topologiesPath);
      for (String topologyId : topologyIds) {
        try {
          GungnirTopology topology = metaStore.findTopologyById(topologyId);
          if (topology.getStatus() == TopologyStatus.RUNNING) {
            addTopology(topologyId);
            curTopologyIds.add(topologyId);
          } else {
            try {
              curator.delete().forPath(topologiesPath + "/" + topologyId);
            } catch (KeeperException.NoNodeException ignore) {
              ignore = null;
            }
          }
        } catch (NotStoredException e) {
          try {
            curator.delete().forPath(topologiesPath + "/" + topologyId);
          } catch (KeeperException.NodeExistsException ignore) {
            ignore = null;
          }
        }
      }

      LOG.info("Join cluster");
    } catch (Exception e) {
      throw new ClusterManagerException(e);
    }
  }

  @Override
  public PersistentDispatcher getDispatcher(String accountId) {
    return dispatchersMap.get(accountId);
  }

  @Override
  public UserEntity getUserByName(String userName) {
    return usersByName.get(userName);
  }

  public void sync(GungnirTopology topology) throws ClusterManagerException {
    try {
      if (topology.getStatus() == TopologyStatus.RUNNING) {
        try {
          curator.create().withMode(CreateMode.PERSISTENT)
              .forPath(topologiesPath + "/" + topology.getId());
        } catch (KeeperException.NodeExistsException ignore) {
          ignore = null;
        }
      } else {
        try {
          curator.delete().forPath(topologiesPath + "/" + topology.getId());
        } catch (KeeperException.NoNodeException ignore) {
          ignore = null;
        }
      }

      LOG.info("Synchronized the cluster. account: {}, topology: {} ({})",
          topology.getOwner().getId(), topology.getId(), topology.getStatus());
    } catch (Exception e) {
      throw new ClusterManagerException(e);
    }
  }


  @Override
  public SessionStore getSessionStore() throws SessionStoreException {
    if (sessionStore == null) {
      sessionStoreLock.lock();
      try {
        if (sessionStore == null) {
          sessionStore = new DistributedSessionStore(curator);
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

    if (curator != null) {
      curator.close();
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
