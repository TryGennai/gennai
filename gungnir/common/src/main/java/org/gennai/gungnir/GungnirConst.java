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

public final class GungnirConst {

  public static final String GUNGNIR_CONFIG = "gungnir.config";
  public static final String GUNGNIR_HOME = "gungnir.home";
  public static final byte[] GUNGNIR_VERSION = {0, 0, 1};
  public static final String GUNGNIR_VERSION_STRING = getVersionString();
  public static final byte[] GUNGNIR_PROTOCOL_VERSION = {0, 1};
  public static final String GUNGNIR_BUILD_TIME = "buildTime";
  public static final String GUNGNIR_CORE_JAR_PREFIX = "gungnir-core-";
  public static final String GUNGNIR_CORE_JAR_SUFFIX = "-jar-with-dependencies.jar";
  public static final String GUNGNIR_TOPOLOGY = "gungnir_";
  public static final String TRACKDATA_TOPIC = "gungnir_track.";
  public static final String LOGGER_NAME = "gungnir";
  public static final String PATH_MARKER_NAME = "@PATH";
  public static final String CONSOLE_GROUP = "console_";

  public static final String ROOT_USER_NAME = "root";
  public static final String ROOT_USER_PASSWORD = "gennai";

  public static final String TUPLE_FIELD = "_tuple";
  public static final String TRACKING_ID_FIELD = "_tid";
  public static final String TRACKING_NO_FIELD = "_tno";
  public static final String ACCEPT_TIME_FIELD = "_time";
  public static final String CONTEXT_FIELD = "_context";

  public static final String GUNGNIR_REST_URI = "/gungnir/v" + getProtocolVersionString();
  public static final String TID_COOKIE_NAME = "TID";
  public static final long SERIAL_VERSION_UID = getVersionNumber();

  public static final String BUILD_PROPERTIES = "/build.properties";

  public static final int TERMINATION_WAIT_TIME = 3000;

  public static final String METRICS_REQUEST_COUNT = "request-count";
  public static final String METRICS_PERSISTENT_DESER_QUEUE_SIZE = "persistent-deser-queue-size";
  public static final String METRICS_PERSISTENT_EMITTER_QUEUE_SIZE =
      "persistent-emitter-queue-size";
  public static final String METRICS_PERSISTENT_DISPATCH_TIME = "persistent-dispatch-time";
  public static final String METRICS_PERSISTENT_DESER_TIME = "persistent-deser-time";
  public static final String METRICS_PERSISTENT_EMIT_TIME = "persistent-emit-time";
  public static final String METRICS_PERSISTENT_DESER_SIZE = "persistent-deser-size";
  public static final String METRICS_PERSISTENT_EMIT_SIZE = "persistent-emit-size";
  public static final String METRICS_PERSISTENT_EMIT_COUNT = "persistent-emit-count";
  public static final String METRICS_DISPATCH_COUNT = "dispatch";
  public static final String METRICS_TUPLE_STORE_SIZE = "store";

  public static final String CLUSTER_NODE_PATH = "/cluster";
  public static final String TOPOLOGIES_NODE_PATH = CLUSTER_NODE_PATH + "/topologies";
  public static final String SERVERS_NODE_PATH = CLUSTER_NODE_PATH + "/servers";
  public static final String STORES_NODE_PATH = CLUSTER_NODE_PATH + "/stores";
  public static final String SESSION_NODE_PATH = CLUSTER_NODE_PATH + "/session";
  public static final String SESSIONS_NODE_PATH = SESSION_NODE_PATH + "/sessions";
  public static final String STATEMENTS_NODE_PATH = SESSION_NODE_PATH + "/statements";
  public static final String SESSION_INDEX_NODE_PATH = SESSION_NODE_PATH + "/index";

  public static final String STORE_DIR = "meta-store";
  public static final String SESSION_CACHE_DIR = "session-cache";
  public static final String TOPOLOGY_CACHE_DIR = "topology-cache";
  public static final String CACHE_DIR = "ttl-cache";

  private GungnirConst() {
  }

  public static long getVersionNumber() {
    long vertionNumber = 0L;
    for (int i = 0; i < GUNGNIR_VERSION.length; i++) {
      long v = GUNGNIR_VERSION[i];
      v *= Math.pow(256, GUNGNIR_VERSION.length - (i + 1));
      vertionNumber += v;
    }
    return vertionNumber;
  }

  private static String getVersionString(byte[] version) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < version.length; i++) {
      if (i > 0) {
        sb.append('.');
      }
      sb.append(version[i]);
    }
    return sb.toString();
  }

  public static String getVersionString() {
    return getVersionString(GUNGNIR_VERSION);
  }

  public static String getProtocolVersionString() {
    return getVersionString(GUNGNIR_PROTOCOL_VERSION);
  }
}
