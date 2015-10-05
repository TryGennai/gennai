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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.finagle.Httpx;
import com.twitter.finagle.Service;
import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.util.DefaultTimer;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;

public class ProxyManager {

  private static final Logger LOG = LoggerFactory.getLogger(ProxyManager.class);

  private static final String DEFAULTS_CONFIG_FILE = "proxy-defaults.yaml";
  private static final String CONFIG_FILE = "proxy.conf.file";
  private static final String CLUSTER_FILE = "proxy.cluster.file";

  static final String PROXY_SERVER_PORT = "proxy.server.port";
  private static final String CONFIG_ZOOKEEPER_SERVERS = "config.zookeeper.servers";
  private static final String CONFIG_ZOOKEEPER_SESSION_TIMEOUT =
      "config.zookeeper.session.timeout";
  private static final String CONFIG_ZOOKEEPER_CONNECTION_TIMEOUT =
      "config.zookeeper.connection.timeout";
  private static final String CONFIG_ZOOKEEPER_RETRY_TIMES = "config.zookeeper.retry.times";
  private static final String CONFIG_ZOOKEEPER_RETRY_INTERVAL = "config.zookeeper.retry.interval";
  private static final String CONFIG_PATH = "config.path";
  private static final String PROXY_TIMEOUT = "proxy.timeout";
  static final String ADMIN_SERVER_PORT = "admin.server.port";
  static final String ADMIN_SERVER_BACKLOG = "admin.server.backlog";

  private static final String CLUSTER_ZOOKEEPER_SERVERS = "zookeeper.servers";
  private static final String CLUSTER_PATH = "path";
  private static final String CLUSTER_TIMEOUT = "timeout";
  private static final String REWRITE_RULES = "rewrite.rules";
  private static final String REWRITE_PATTERN = "pattern";
  private static final String REWRITE_TARGET = "target";

  private static final class RewriteRule {

    private Pattern pattern;
    private String target;

    private RewriteRule(String pattern, String target) {
      this.pattern = Pattern.compile(pattern);
      this.target = target;
    }

    @Override
    public String toString() {
      return "'" + pattern + "' -> '" + target + "'";
    }
  }

  private static final class Cluster {

    private String dest;
    private Service<Request, Response> client;
    private int timeout;
    private List<RewriteRule> rewriteRules;

    private Cluster(String dest, int timeout, List<RewriteRule> rewriteRules) {
      this.dest = dest;
      this.timeout = timeout;
      this.rewriteRules = rewriteRules;
    }

    private void prepare() {
      cleanup();
      client = Httpx.newService(dest);
      LOG.debug("prepare {}", this);
    }

    private void prepare(Cluster cluster) {
      client = cluster.client;
      LOG.debug("reprepare {}", cluster);
    }

    private void cleanup() {
      if (client != null) {
        client.close();
        LOG.debug("cleanup {}", this);
      }
    }

    @Override
    public String toString() {
      return "dest:" + dest + ", timeout:" + timeout + ", rewrite rules:" + rewriteRules;
    }
  }

  private Map<String, Object> config;
  private String configPath;
  private int proxyTimeout;
  private List<Cluster> clusters;
  private CuratorFramework curator;
  private ReentrantReadWriteLock syncLock;
  private ObjectMapper mapper;

  private static Map<String, Object> readConfig() {
    Map<String, Object> config = Maps.newHashMap();

    Yaml yaml = new Yaml(new SafeConstructor());

    InputStream is = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(DEFAULTS_CONFIG_FILE);
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> ret = (Map<String, Object>) yaml.load(new InputStreamReader(is));
      config.putAll(ret);
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        LOG.warn("Failed to close " + DEFAULTS_CONFIG_FILE);
      }
    }

    String confFile = System.getProperty(CONFIG_FILE);
    if (confFile != null && !confFile.isEmpty()) {
      BufferedReader reader = null;
      try {
        reader = Files.newBufferedReader(Paths.get(confFile), StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, Object> ret = (Map<String, Object>) yaml.load(reader);
        if (ret != null) {
          config.putAll(ret);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read " + confFile);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.warn("Failed to close " + confFile);
          }
        }
      }
    }

    return config;
  }

  public ProxyManager() {
    config = readConfig();
    configPath = (String) config.get(CONFIG_PATH);
    proxyTimeout = (Integer) config.get(PROXY_TIMEOUT);
    clusters = Lists.newArrayList();
    syncLock = new ReentrantReadWriteLock();
    mapper = new ObjectMapper();
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public synchronized void readClusterConfig() {
    Set<Map<String, Object>> clusterConfigs = null;
    try {
      byte[] bytes = curator.getData().watched().forPath(configPath);
      clusterConfigs = mapper.readValue(bytes,
          mapper.getTypeFactory().constructCollectionType(HashSet.class,
              mapper.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class)));
    } catch (Exception e) {
      LOG.error("Failed to read cluster config", e);
    }

    if (clusterConfigs != null) {
      List<Cluster> newClusters = Lists.newArrayList();
      for (Map<String, Object> clusterConfig : clusterConfigs) {
        @SuppressWarnings("unchecked")
        List<String> zkServers = (List<String>) clusterConfig.get(CLUSTER_ZOOKEEPER_SERVERS);
        if (zkServers == null) {
          @SuppressWarnings("unchecked")
          List<String> servers = (List<String>) config.get(CONFIG_ZOOKEEPER_SERVERS);
          zkServers = servers;
        }
        String dest = "zk!" + StringUtils.join(zkServers, ",") + "!"
            + clusterConfig.get(CLUSTER_PATH);

        Integer timeout = (Integer) clusterConfig.get(CLUSTER_TIMEOUT);
        if (timeout == null) {
          timeout = proxyTimeout;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, String>> rules =
            (List<Map<String, String>>) clusterConfig.get(REWRITE_RULES);
        List<RewriteRule> rewriteRules = Lists.newArrayList();
        if (rules != null) {
          for (Map<String, String> rule : rules) {
            String pattern = (String) rule.get(REWRITE_PATTERN);
            String target = (String) rule.get(REWRITE_TARGET);
            if (pattern != null && target != null) {
              rewriteRules.add(new RewriteRule(pattern, target));
            }
          }
        }

        newClusters.add(new Cluster(dest, timeout, rewriteRules));
      }

      WriteLock writeLock = syncLock.writeLock();
      writeLock.lock();
      try {
        for (Cluster newCluster : newClusters) {
          Cluster reuse = null;
          for (int i = 0; i < clusters.size(); i++) {
            if (newCluster.dest.equals(clusters.get(i).dest)
                && newCluster.timeout == clusters.get(i).timeout) {
              reuse = clusters.remove(i);
              break;
            }
          }
          if (reuse != null) {
            newCluster.prepare(reuse);
          } else {
            newCluster.prepare();
          }
        }

        for (Cluster cluster : clusters) {
          cluster.cleanup();
        }

        clusters = newClusters;
      } finally {
        writeLock.unlock();
      }

      for (Cluster cluster : clusters) {
        LOG.info("Read cluster config. {}", cluster);
      }
    }
  }

  public void writeClusterConfig() throws Exception {
    List<Map<String, Object>> clusterConfigs = null;

    Yaml yaml = new Yaml(new SafeConstructor());

    String clusterFile = System.getProperty(CLUSTER_FILE);
    if (clusterFile != null && !clusterFile.isEmpty()) {
      BufferedReader reader = null;
      try {
        reader = Files.newBufferedReader(Paths.get(clusterFile), StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> ret = (List<Map<String, Object>>) yaml.load(reader);
        clusterConfigs = ret;
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.warn("Failed to close " + clusterFile);
          }
        }
      }
    }

    if (clusterConfigs != null) {
      if (curator.checkExists().forPath(configPath) == null) {
        try {
          curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(configPath);
        } catch (KeeperException.NodeExistsException ignore) {
          ignore = null;
        }
      }

      curator.setData().forPath(configPath, mapper.writeValueAsBytes(clusterConfigs));

      LOG.info("Write cluster config. {}", clusterConfigs);
    }
  }

  public void start() throws Exception {
    @SuppressWarnings("unchecked")
    List<String> zkServers = (List<String>) config.get(CONFIG_ZOOKEEPER_SERVERS);

    curator = CuratorFrameworkFactory.builder()
        .connectString(StringUtils.join(zkServers, ","))
        .sessionTimeoutMs((Integer) config.get(CONFIG_ZOOKEEPER_SESSION_TIMEOUT))
        .connectionTimeoutMs((Integer) config.get(CONFIG_ZOOKEEPER_CONNECTION_TIMEOUT))
        .retryPolicy(new RetryNTimes((Integer) config.get(CONFIG_ZOOKEEPER_RETRY_TIMES),
            (Integer) config.get(CONFIG_ZOOKEEPER_RETRY_INTERVAL))).build();

    curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {

      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        LOG.info("Connection state changed. state: {}", newState);
      }
    });

    curator.getCuratorListenable().addListener(new CuratorListener() {

      @Override
      public void eventReceived(CuratorFramework curator, CuratorEvent event) throws Exception {
        if (event.getType() == CuratorEventType.WATCHED
            && event.getWatchedEvent().getType() == EventType.NodeDataChanged
            && event.getWatchedEvent().getPath().equals(configPath)) {
          readClusterConfig();
        }
      }
    });

    curator.start();
  }

  private Request rewrite(List<RewriteRule> rewriteRules, Request request) {
    if (!rewriteRules.isEmpty()) {
      for (RewriteRule rewriteRule : rewriteRules) {
        Matcher matcher = rewriteRule.pattern.matcher(request.getUri());
        if (matcher.find()) {
          Request requestCopy = Request.apply(request.version(), request.method(),
              matcher.replaceAll(rewriteRule.target));
          requestCopy.headers().add(request.headers());
          requestCopy.setContent(request.getContent());
          requestCopy.setChunked(request.isChunked());
          return requestCopy;
        }
      }
    }
    return request;
  }

  public void send(Request request) {
    ReadLock readLock = syncLock.readLock();
    readLock.lock();
    try {
      for (Cluster cluster : clusters) {
        try {
          Await.ready(cluster.client.apply(rewrite(cluster.rewriteRules, request))
              .raiseWithin(new Duration(TimeUnit.MILLISECONDS.toNanos(cluster.timeout)),
                  DefaultTimer.twitter()).addEventListener(new FutureEventListener<Response>() {

                @Override
                public void onFailure(Throwable cause) {
                  Stats.incr("failed");
                  LOG.error("Failed to send request", cause);
                }

                @Override
                public void onSuccess(Response response) {
                }
              }));
        } catch (TimeoutException e) {
          Stats.incr("timeout");
          LOG.error("Send request timed out", e);
        } catch (Exception e) {
          Stats.incr("failed");
          LOG.error("Failed to send request", e);
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  public void close() {
    WriteLock writeLock = syncLock.writeLock();
    writeLock.lock();
    try {
      for (Cluster cluster : clusters) {
        cluster.cleanup();
      }
      clusters.clear();
    } finally {
      writeLock.unlock();
    }

    if (curator != null) {
      curator.close();
    }
  }
}
