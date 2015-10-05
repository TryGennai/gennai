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

package org.gennai.gungnir.server;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.cluster.ClusterManager;
import org.gennai.gungnir.cluster.ClusterManagerException;
import org.gennai.gungnir.cluster.storm.CapacityWorkerException;
import org.gennai.gungnir.cluster.storm.StormClusterManager;
import org.gennai.gungnir.cluster.storm.StormClusterManagerException;
import org.gennai.gungnir.cluster.storm.TopologyStatusChangedListener;
import org.gennai.gungnir.metastore.InMemoryMetaStore;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.tuple.persistent.InMemoryEmitter;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.SLF4JHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

import com.twitter.finagle.Announcement;
import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.TimeoutException;

public class GungnirServer {

  private static final Logger LOG = LoggerFactory.getLogger(GungnirServer.class);

  private GungnirManager manager;
  private GungnirConfig config;
  private MetaStore metaStore;
  private ClusterManager clusterManager;
  private ListeningServer gungnirServer;
  private Announcement gsAnm;
  private ExecutorService gungnirServerExecutor;
  private ListeningServer tupleStoreServer;
  private Announcement tssAnm;
  private ExecutorService tupleStoreExecutor;
  private OstrichAdminService adminService;

  public GungnirServer() throws MetaStoreException {
    manager = GungnirManager.getManager();
    config = manager.getConfig();
    metaStore = manager.getMetaStore();
  }

  public GungnirConfig getConfig() {
    return config;
  }

  private void initMetaStore() throws MetaStoreException {
    String clusterMode = config.getString(CLUSTER_MODE);
    if (metaStore instanceof InMemoryMetaStore && !clusterMode.equals(LOCAL_CLUSTER)) {
      LOG.error("In-memory metastore can't be used in cluster other than the local cluster");
      throw new MetaStoreException(
          "In-memory metastore can't be used in cluster other than the local cluster");
    }

    metaStore.init();
  }

  public void startClusterManager() throws ClusterManagerException {
    clusterManager = manager.getClusterManager();
    clusterManager.start();
  }

  private void restartTopologies() throws MetaStoreException, StormClusterManagerException,
      CapacityWorkerException {
    int cnt = 0;
    List<UserEntity> accounts = metaStore.findUserAccounts();
    for (UserEntity account : accounts) {
      List<GungnirTopology> topologies = metaStore.findTopologies(account, TopologyStatus.RUNNING);
      cnt += topologies.size();
    }

    final CountDownLatch restarted = new CountDownLatch(cnt);
    for (UserEntity account : accounts) {
      List<GungnirTopology> topologies = metaStore.findTopologies(account, TopologyStatus.RUNNING);
      for (final GungnirTopology topology : topologies) {
        topology.setStatus(TopologyStatus.STARTING);
        StormClusterManager.getManager().startTopology(topology,
            new TopologyStatusChangedListener() {

              @Override
              public void process() {
                topology.setStatus(TopologyStatus.RUNNING);
                try {
                  manager.getClusterManager().sync(topology);
                  LOG.info("Successful to restart topology '{}'", topology.getId());
                } catch (Exception e) {
                  LOG.error("Failed to restart topology '{}'", topology.getId());
                }
                restarted.countDown();
              }

              @Override
              public void rollback() {
                restarted.countDown();
              }
            });
      }
    }

    try {
      restarted.await();
    } catch (InterruptedException e) {
      LOG.error("Failed to restart topologies", e);
    }
  }

  private void startStormCluster() throws MetaStoreException, StormClusterManagerException,
      CapacityWorkerException {
    if (config.getString(STORM_CLUSTER_MODE).equals(LOCAL_CLUSTER)) {
      StormClusterManager.getManager().startLocalCluster();
      restartTopologies();
    }
  }

  public void executeServer() {
    gungnirServer =
        Thrift.serveIface(new InetSocketAddress(config.getInteger(GUNGNIR_SERVER_PORT)),
            new GungnirServiceProcessor());

    if (config.getString(CLUSTER_MODE).equals(DISTRIBUTED_CLUSTER)) {
      List<String> zkServers = config.getList(CLUSTER_ZOOKEEPER_SERVERS);
      try {
        gsAnm =
            Await.result(gungnirServer.announce("zk!" + StringUtils.join(zkServers, ",") + "!"
                + config.getString(GUNGNIR_NODE_PATH) + SERVERS_NODE_PATH + "!0"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    gungnirServerExecutor =
        Executors.newSingleThreadExecutor(GungnirUtils.createThreadFactory("GungnirServer"));
    gungnirServerExecutor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          Await.ready(gungnirServer);
        } catch (TimeoutException e) {
          LOG.error("Gungnir server timed out");
        } catch (InterruptedException e) {
          LOG.info("Gungnir server interrupted");
        }
      }
    });

    LOG.info("Gungnir server started");
  }

  public void executeTupleStore() throws ClusterManagerException {
    try {
      if (config.getClass(GungnirConfig.PERSISTENT_EMITTER) == InMemoryEmitter.class
          && (!config.getString(CLUSTER_MODE).equals(LOCAL_CLUSTER)
          || !config.getString(STORM_CLUSTER_MODE).equals(LOCAL_CLUSTER))) {
        LOG.error("Memory queue can't be used in cluster other than the storm local cluster");
        throw new RuntimeException(
            "Memory queue can't be used in cluster other than the storm local cluster");
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      tupleStoreServer =
          Http.serve(new InetSocketAddress(config.getInteger(TUPLE_STORE_SERVER_PORT)),
              new TupleStoreService());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    clusterManager.join();

    if (config.getString(CLUSTER_MODE).equals(DISTRIBUTED_CLUSTER)) {
      List<String> zkServers = config.getList(CLUSTER_ZOOKEEPER_SERVERS);
      try {
        tssAnm = Await.result(tupleStoreServer.announce("zk!" + StringUtils.join(zkServers, ",")
            + "!" + config.getString(GUNGNIR_NODE_PATH) + STORES_NODE_PATH + "!0"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    tupleStoreExecutor =
        Executors.newSingleThreadExecutor(GungnirUtils.createThreadFactory("TupleStoreServer"));
    tupleStoreExecutor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          Await.ready(tupleStoreServer);
        } catch (TimeoutException e) {
          LOG.error("Tuple store server timed out");
        } catch (InterruptedException e) {
          LOG.info("Tuple store server interrupted");
        }
      }
    });

    LOG.info("Tuple store server started");
  }

  public void executeAdminService() {
    Integer port = config.getInteger(GUNGNIR_ADMIN_SERVER_PORT);
    Integer backlog = config.getInteger(GUNGNIR_ADMIN_SERVER_BACKLOG);
    if (port != null && backlog != null) {
      adminService = new OstrichAdminService(port, backlog);
      adminService.start();
    }
  }

  public void close() {
    if (adminService != null) {
      adminService.shutdown();
      LOG.info("Admin server closed");
    }

    if (tupleStoreExecutor != null) {
      tupleStoreExecutor.shutdownNow();
      LOG.info("Tuple store server shutdown");
    }

    if (tupleStoreServer != null) {
      try {
        if (tssAnm != null) {
          Await.result(tssAnm.unannounce());
        }
        Await.result(tupleStoreServer.close());
        LOG.info("Tuple store server closed");
      } catch (Exception e) {
        LOG.error("Failed to close tuple store server", e);
      }
    }

    if (gungnirServerExecutor != null) {
      gungnirServerExecutor.shutdownNow();
      LOG.info("Gungnir server shutdown");
    }

    if (gungnirServer != null) {
      try {
        if (gsAnm != null) {
          Await.result(gsAnm.unannounce());
        }
        Await.result(gungnirServer.close());
      } catch (Exception e) {
        LOG.error("Failed to close gungnir server", e);
      }
    }

    if (config.getString(STORM_CLUSTER_MODE).equals(LOCAL_CLUSTER)) {
      StormClusterManager.getManager().shutdownLocalCluster();
    }

    if (clusterManager != null) {
      clusterManager.close();
    }

    if (manager != null) {
      manager.close();
    }

    ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();

    LOG.info("Gungnir server closed");
  }

  public static void main(String[] args) throws MetaStoreException {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.twitter");
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JHandler());

    final GungnirServer gungnirServer = new GungnirServer();

    try {
      gungnirServer.initMetaStore();

      gungnirServer.startClusterManager();

      gungnirServer.startStormCluster();

      gungnirServer.executeServer();

      if (gungnirServer.getConfig().getString(CLUSTER_MODE).equals(LOCAL_CLUSTER)) {
        gungnirServer.executeTupleStore();
        gungnirServer.executeAdminService();
      }

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          gungnirServer.close();
        }
      });
    } catch (Exception e) {
      LOG.error("Failed to start gungnir server", e);
      if (gungnirServer != null) {
        gungnirServer.close();
      }
    }
  }
}
