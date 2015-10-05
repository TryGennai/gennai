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

import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.utils.SLF4JHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TupleStoreServer {

  private static final Logger LOG = LoggerFactory.getLogger(TupleStoreServer.class);

  private TupleStoreServer() {
  }

  public static void main(String[] args) throws MetaStoreException {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.twitter");
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JHandler());

    final GungnirServer gungnirServer = new GungnirServer();

    String clusterMode = gungnirServer.getConfig().getString(CLUSTER_MODE);
    if (clusterMode.equals(LOCAL_CLUSTER)) {
      LOG.error("When cluster.mode isn't 'distributed', the tuple store server can't start");
      System.exit(1);
    }

    try {
      gungnirServer.startClusterManager();

      gungnirServer.executeTupleStore();

      gungnirServer.executeAdminService();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          gungnirServer.close();
        }
      });
    } catch (Exception e) {
      LOG.error("Failed to start tuple store server", e);
      if (gungnirServer != null) {
        gungnirServer.close();
      }
    }
  }
}
