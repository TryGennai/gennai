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

import static java.util.logging.Level.*;
import static org.gennai.gungnir.server.ProxyManager.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map$;
import scala.util.matching.Regex;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.finagle.Httpx;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.httpx.Status;
import com.twitter.ostrich.admin.AdminHttpService;
import com.twitter.ostrich.admin.AdminServiceFactory;
import com.twitter.ostrich.admin.CustomHttpHandler;
import com.twitter.ostrich.admin.RuntimeEnvironment;
import com.twitter.ostrich.admin.StatsFactory;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import com.twitter.util.TimeoutException;

public final class ProxyServer {

  private static final Logger LOG = LoggerFactory.getLogger(ProxyServer.class);

  public static ThreadFactory createThreadFactory(String name) {
    return new ThreadFactoryBuilder().setNameFormat(name + "-%d")
        .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

          @Override
          public void uncaughtException(Thread t, Throwable e) {
            LOG.error("Uncaugh exception has occurred", e);
          }
        }).build();
  }

  private static final class ProxyService extends Service<Request, Response> {

    private ProxyManager manager;
    private Executor executor;

    private ProxyService(ProxyManager manager) {
      this.manager = manager;
      executor = Executors.newCachedThreadPool(createThreadFactory("ProxyService"));
    }

    @Override
    public Future<Response> apply(final Request request) {
      final Promise<Response> promise = new Promise<Response>();

      executor.execute(new Runnable() {

        @Override
        public void run() {
          manager.send(request);
          promise.setValue(Response.apply(request.version(), Status.NoContent()));
        }
      });

      return promise;
    }
  }

  private ProxyServer() {
  }

  static AdminHttpService executeAdminService(Map<String, Object> config) {
    Integer port = (Integer) config.get(ADMIN_SERVER_PORT);
    Integer backlog = (Integer) config.get(ADMIN_SERVER_BACKLOG);

    if (port != null && backlog != null) {
      List<Duration> defaultLatchIntervals = JavaConverters.asScalaBufferConverter(
          Lists.<Duration>newArrayList(Duration.apply(1, TimeUnit.MINUTES))).asScala().toList();

      return new AdminServiceFactory(
          port,
          backlog,
          List$.MODULE$.<StatsFactory>empty(),
          Option.<String>empty(),
          List$.MODULE$.<Regex>empty(),
          Map$.MODULE$.<String, CustomHttpHandler>empty(),
          defaultLatchIntervals)
          .apply(new RuntimeEnvironment(config));
    }

    return null;
  }

  static class SLF4JHandler extends Handler {

    @Override
    public void publish(LogRecord record) {
      Logger logger = LoggerFactory.getLogger(record.getLoggerName());
      Throwable throwable = record.getThrown();
      if (record.getLevel().equals(SEVERE)) {
        logger.error(record.getMessage(), throwable);
      } else if (record.getLevel().equals(WARNING)) {
        logger.warn(record.getMessage(), throwable);
      } else if (record.getLevel().equals(INFO) || record.getLevel().equals(CONFIG)) {
        logger.info(record.getMessage(), throwable);
      } else if (record.getLevel().equals(FINE)) {
        logger.debug(record.getMessage(), throwable);
      } else if (record.getLevel().equals(FINER) || record.getLevel().equals(FINEST)) {
        logger.trace(record.getMessage(), throwable);
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }

  public static void main(String[] args) throws Exception {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.twitter");
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JHandler());

    final ProxyManager manager = new ProxyManager();
    if (args.length == 0) {
      AdminHttpService adminService = null;
      try {
        manager.start();
        manager.writeClusterConfig();
        manager.readClusterConfig();

        adminService = executeAdminService(manager.getConfig());

        final ListeningServer server = Httpx.serve(
            new InetSocketAddress((Integer) manager.getConfig().get(PROXY_SERVER_PORT)),
            new ProxyService(manager));

        ExecutorService proxyServerExecutor =
            Executors.newSingleThreadExecutor(createThreadFactory("ProxyServer"));
        proxyServerExecutor.execute(new Runnable() {

          @Override
          public void run() {
            try {
              Await.ready(server);
            } catch (TimeoutException e) {
              LOG.error("Proxy server timed out");
            } catch (InterruptedException e) {
              LOG.info("Proxy server interrupted");
            }
          }
        });

        LOG.info("Proxy server started");

        Runtime.getRuntime().addShutdownHook(new Thread() {

          private AdminHttpService adminService;

          private Thread setAdminService(AdminHttpService adminService) {
            this.adminService = adminService;
            return this;
          }

          @Override
          public void run() {
            if (adminService != null) {
              adminService.shutdown();
            }

            manager.close();
          }
        }.setAdminService(adminService));
      } catch (Exception e) {
        LOG.error("Failed to start proxy server", e);
        if (adminService != null) {
          adminService.shutdown();
        }

        manager.close();
      }
    } else if (args.length == 1 && "reload".equals(args[0])) {
      try {
        manager.start();
        manager.writeClusterConfig();
      } finally {
        manager.close();
      }
    }
  }
}
