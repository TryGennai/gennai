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

package org.gennai.gungnir.cluster.storm;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.storm.guava.collect.Lists;
import org.apache.thrift7.TException;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.tuple.serialization.SerializationRegistry;
import org.gennai.gungnir.utils.GungnirUtils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public final class StormClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(StormClusterManager.class);

  private static volatile StormClusterManager MANAGER;

  private Config stormConf;
  private LocalCluster localCluster;

  private class ClusterDriver {

    private Config stormConf;
    private NimbusClient nimbusClient;

    ClusterDriver(Config stormConf) {
      this.stormConf = stormConf;
      if (localCluster == null) {
        stormConf.put(Config.STORM_CLUSTER_MODE, DISTRIBUTED_CLUSTER);

        LOG.debug("Nimbus client {}: {}, {}: {}", Config.NIMBUS_HOST,
            stormConf.get(Config.NIMBUS_HOST), Config.NIMBUS_THRIFT_PORT,
            stormConf.get(Config.NIMBUS_THRIFT_PORT));

        nimbusClient = NimbusClient.getConfiguredClient(stormConf);
      } else {
        stormConf.put(Config.STORM_CLUSTER_MODE, LOCAL_CLUSTER);
      }
    }

    void submitTopology(String topologyName, StormTopology stormTopology, Path submitJar)
        throws AlreadyAliveException, InvalidTopologyException, TException {
      if (localCluster == null) {
        String serConf = JSONValue.toJSONString(stormConf);

        LOG.debug("Submit jar '{}'", submitJar);
        String location = StormSubmitter.submitJar(stormConf, submitJar.toString());
        LOG.info("Submitted jar '{}'", location);

        nimbusClient.getClient().submitTopology(topologyName, location, serConf, stormTopology);

        LOG.info("Topology '{}' was submitted in distributed mode", topologyName);
      } else {
        localCluster.submitTopology(topologyName, stormConf, stormTopology);

        LOG.info("Topology '{}' was submitted in local mode", topologyName);
      }
    }

    void killTopology(String topologyName) throws TException, NotAliveException {
      KillOptions killOptions = new KillOptions();
      killOptions.set_wait_secs(0);
      if (localCluster == null) {
        nimbusClient.getClient().killTopologyWithOpts(topologyName, killOptions);

        LOG.info("Topology '{}' was killed in distributed mode", topologyName);
      } else {
        localCluster.killTopologyWithOpts(topologyName, killOptions);

        LOG.info("Topology '{}' was killed in local mode", topologyName);
      }
    }

    ClusterSummary getClusterInfo() throws TException {
      if (localCluster == null) {
        return nimbusClient.getClient().getClusterInfo();
      } else {
        return localCluster.getClusterInfo();
      }
    }

    boolean isWorkerFull() throws TException {
      ClusterSummary clusterSummary = getClusterInfo();

      int numWorkers = 0;
      int numUsedWorkers = 0;
      for (SupervisorSummary summary : clusterSummary.get_supervisors()) {
        numWorkers += summary.get_num_workers();
        numUsedWorkers += summary.get_num_used_workers();
      }

      return (numUsedWorkers >= numWorkers);
    }

    boolean isSubmitted(String topologyName) {
      try {
        ClusterSummary clusterSummary = getClusterInfo();
        for (TopologySummary topologySummary : clusterSummary.get_topologies()) {
          if (topologySummary.get_name().equals(topologyName)) {
            return true;
          }
        }
      } catch (Exception e) { // TODO NullPointerException occurred in the nimbus
        LOG.error("Failed to get cluster info", e);
      }
      return false;
    }

    boolean isActivated(String topologyName) {
      try {
        ClusterSummary clusterSummary = getClusterInfo();
        for (TopologySummary topologySummary : clusterSummary.get_topologies()) {
          if (topologySummary.get_name().equals(topologyName)
              && "ACTIVE".equals(topologySummary.get_status())) {
            return true;
          }
        }
      } catch (Exception e) { // TODO NullPointerException occurred in the nimbus
        LOG.error("Failed to get cluster info", e);
      }
      return false;
    }

    TopologyInfo getTopologyInfo(String topologyId) throws TException, NotAliveException {
      if (localCluster == null) {
        return nimbusClient.getClient().getTopologyInfo(topologyId);
      } else {
        return localCluster.getTopologyInfo(topologyId);
      }
    }

    void close() {
      if (nimbusClient != null) {
        nimbusClient.close();
        nimbusClient = null;
      }
    }
  }

  private class StatusChecker implements Runnable {

    private ClusterDriver driver;
    private GungnirTopology topology;
    private TopologyStatusChangedListener listener;

    public StatusChecker(ClusterDriver driver, GungnirTopology topology,
        TopologyStatusChangedListener listener) {
      this.driver = driver;
      this.topology = topology;
      this.listener = listener;
    }

    @Override
    public void run() {
      GungnirConfig config = GungnirManager.getManager().getConfig();
      long times = config.getInteger(TOPOLOGY_STATUS_CHECK_TIMES);
      long interval = config.getInteger(TOPOLOGY_STATUS_CHECK_INTERVAL);

      boolean changed = false;
      int cnt = 0;
      try {
        for (; cnt < times; cnt++) {
          switch (topology.getStatus()) {
            case STARTING:
              if (driver.isActivated(getStormTopologyName(topology))) {
                changed = true;
              }
              break;
            case STOPPING:
              if (!driver.isSubmitted(getStormTopologyName(topology))) {
                changed = true;
              }
              break;
            default:
          }

          if (changed) {
            break;
          }

          TimeUnit.MILLISECONDS.sleep(interval);
        }
      } catch (InterruptedException e) {
        LOG.info("Topology status check interrupted");
      }

      if (cnt < times) {
        listener.process();
      } else {
        LOG.error("Topology status check timed out '{}'", topology.getId());

        listener.rollback();
        LOG.info("Rollback topology");
      }

      driver.close();
    }
  }

  @SuppressWarnings("unchecked")
  private StormClusterManager() {
    stormConf = new Config();
    stormConf.putAll(Utils.readStormConfig());

    GungnirConfig config = GungnirManager.getManager().getConfig();
    stormConf.put(Config.NIMBUS_HOST, config.get(STORM_NIMBUS_HOST));
    SerializationRegistry.putConf(stormConf);
  }

  public static StormClusterManager getManager() {
    if (MANAGER == null) {
      synchronized (StormClusterManager.class) {
        if (MANAGER == null) {
          MANAGER = new StormClusterManager();
        }
      }
    }
    return MANAGER;
  }

  public static String getStormTopologyName(GungnirTopology topology) {
    return GUNGNIR_TOPOLOGY + topology.getId();
  }

  public ClusterSummary getClusterInfo() throws TException {
    ClusterDriver driver = new ClusterDriver(stormConf);
    try {
      return driver.getClusterInfo();
    } finally {
      driver.close();
    }
  }

  public TopologyInfo getTopologyInfo(String topologyId) throws TException, NotAliveException {
    ClusterDriver driver = new ClusterDriver(stormConf);
    try {
      return driver.getTopologyInfo(topologyId);
    } finally {
      driver.close();
    }
  }

  private void submitTopology(ClusterDriver driver, GungnirTopology topology,
      StormTopology stormTopology, Path classPath, TopologyStatusChangedListener listener) {
    try {
      Path submitJar = null;
      Path srcJar = null;
      if (localCluster == null) {
        srcJar = Paths.get(System.getProperty(GUNGNIR_HOME), GUNGNIR_CORE_JAR_PREFIX
            + GUNGNIR_VERSION_STRING + GUNGNIR_CORE_JAR_SUFFIX);
        List<Path> addPaths = Lists.newArrayList(Files.newDirectoryStream(classPath));
        if (addPaths.isEmpty()) {
          submitJar = srcJar;
        } else {
          submitJar = Paths.get(topology.getConfig().getString(LOCAL_DIR), TOPOLOGY_CACHE_DIR,
              topology.getId() + GUNGNIR_CORE_JAR_SUFFIX);
          GungnirUtils.createFatJar(srcJar, addPaths, submitJar);
        }
      }

      driver.submitTopology(getStormTopologyName(topology), stormTopology, submitJar);


      if (localCluster == null && !submitJar.equals(srcJar)) {
        try {
          Files.deleteIfExists(submitJar);
        } catch (IOException e) {
          LOG.error("Failed delete to the submit jar");
        }
      }

      ExecutorService executor = Executors.newSingleThreadExecutor(
          GungnirUtils.createThreadFactory("ClusterManager"));
      executor.execute(new StatusChecker(driver, topology, listener));
    } catch (AlreadyAliveException e) {
      listener.rollback();
      driver.close();
      LOG.error("Failed to submit topology '{}'", topology.getId(), e);
    } catch (InvalidTopologyException e) {
      listener.rollback();
      driver.close();
      LOG.error("Failed to submit topology '{}'", topology.getId(), e);
    } catch (TException e) {
      listener.rollback();
      driver.close();
      LOG.error("Failed to submit topology '{}'", topology.getId(), e);
    } catch (IOException e) {
      listener.rollback();
      driver.close();
      LOG.error("Failed to submit topology '{}'", topology.getId(), e);
    }
  }

  private class TopologySubmitter implements Runnable {

    private ClusterDriver driver;
    private GungnirTopology topology;
    private StormTopology stormTopology;
    private Path classPath;
    private TopologyStatusChangedListener listener;

    TopologySubmitter(ClusterDriver driver, GungnirTopology topology, StormTopology stormTopology,
        Path classPath, TopologyStatusChangedListener listener) {
      this.driver = driver;
      this.topology = topology;
      this.stormTopology = stormTopology;
      this.classPath = classPath;
      this.listener = listener;
    }

    @Override
    public void run() {
      submitTopology(driver, topology, stormTopology, classPath, listener);
    }
  }

  private void doSubmitTopology(ClusterDriver driver, GungnirTopology topology,
      StormTopology stormTopology, Path classPath, TopologyStatusChangedListener listener) {
    if (localCluster == null) {
      ExecutorService executor = Executors.newSingleThreadExecutor(
          GungnirUtils.createThreadFactory("ClusterManager"));
      executor.execute(new TopologySubmitter(driver, topology, stormTopology, classPath, listener));
    } else {
      submitTopology(driver, topology, stormTopology, classPath, listener);
    }
  }

  public void startTopology(GungnirTopology topology, TopologyStatusChangedListener listener)
      throws StormClusterManagerException, CapacityWorkerException {
    LOG.debug("config: {}", topology.getConfig());

    Path classPath = Paths.get(topology.getConfig().getString(LOCAL_DIR), TOPOLOGY_CACHE_DIR,
        topology.getId());

    Config stormConfCopy = new Config();
    stormConfCopy.putAll(stormConf);

    GungnirConfig configCopy = topology.getConfig().clone();
    configCopy.put(CLASS_PATH, classPath.toString());

    stormConfCopy.setNumWorkers(configCopy.getInteger(TOPOLOGY_WORKERS));
    stormConfCopy.put(GUNGNIR_CONFIG, configCopy);
    if (configCopy.getBoolean(TOPOLOGY_METRICS_ENABLED)) {
      try {
        stormConfCopy.registerMetricsConsumer(configCopy.getClass(TOPOLOGY_METRICS_CONSUMER),
            configCopy.getInteger(TOPOLOGY_METRICS_CONSUMER_PARALLELISM));
      } catch (ClassNotFoundException e) {
        LOG.error("Invalid metrics consumer class", e);
      }
    }
    stormConfCopy.setStatsSampleRate(configCopy.getDouble(TOPOLOGY_STATS_SAMPLE_RATE));

    ClusterDriver driver = new ClusterDriver(stormConfCopy);

    try {
      if (driver.isWorkerFull()) {
        LOG.warn("Worker slots is full");
        driver.close();
        throw new CapacityWorkerException("Worker slots is full");
      }
    } catch (TException e) {
      LOG.error("Failed to get cluster info", e);
      driver.close();
      throw new StormClusterManagerException("Failed to get cluster info", e);
    }

    try {
      StormTopology stormTopology = topology.build();
      doSubmitTopology(driver, topology, stormTopology, classPath, listener);
    } catch (GungnirTopologyException e) {
      LOG.error("Failed to build topology '{}'", topology.getId(), e);
      throw new StormClusterManagerException(e);
    }

    LOG.info("Successful to start topology '{}'", topology.getId());
  }

  public void stopTopology(GungnirTopology topology, TopologyStatusChangedListener listener)
      throws NotAliveException, StormClusterManagerException {
    ClusterDriver driver = new ClusterDriver(stormConf);
    try {
      driver.killTopology(getStormTopologyName(topology));
    } catch (TException e) {
      LOG.error("Failed to kill topology '{}'", topology.getId(), e);
      throw new StormClusterManagerException("Failed to kill topology", e);
    }

    ExecutorService executor = Executors.newSingleThreadExecutor(
        GungnirUtils.createThreadFactory("ClusterManager"));
    executor.execute(new StatusChecker(driver, topology, listener));
    LOG.info("Successful to stop topology '{}'", topology.getId());
  }

  private static class TopologyStoppedListener implements TopologyStatusChangedListener {

    private boolean results;
    private CountDownLatch replied = new CountDownLatch(1);

    @Override
    public void process() {
      results = true;
      replied.countDown();
    }

    @Override
    public void rollback() {
      results = false;
      replied.countDown();
    }

    private boolean results() throws InterruptedException {
      replied.await();
      return results;
    }
  }

  public boolean stopTopologySync(GungnirTopology topology) throws NotAliveException,
      StormClusterManagerException, InterruptedException {
    TopologyStoppedListener listener = new TopologyStoppedListener();
    stopTopology(topology, listener);
    return listener.results();
  }

  public void startLocalCluster() {
    if (localCluster == null) {
      localCluster = new LocalCluster();
      LOG.info("Local cluster started");
    }
  }

  public void shutdownLocalCluster() {
    if (localCluster != null) {
      localCluster.shutdown();
      LOG.info("Local cluster shutdown");
    }
  }

  public boolean isLocalCluster() {
    if (localCluster != null) {
      return true;
    }
    return false;
  }
}
