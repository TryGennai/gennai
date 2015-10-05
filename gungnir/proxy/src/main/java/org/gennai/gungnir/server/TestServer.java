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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.gennai.gungnir.server.ProxyServer.SLF4JHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.Httpx;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.httpx.Cookie;
import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.httpx.Status;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.TimeoutException;

public final class TestServer {

  private static final Logger LOG = LoggerFactory.getLogger(TestServer.class);

  public static class HttpServer extends Service<Request, Response> {

    private AtomicInteger cnt;
    private ExecutorService metricsExecutor;

    public HttpServer() {
      cnt = new AtomicInteger();
      metricsExecutor = Executors.newSingleThreadExecutor();
      metricsExecutor.execute(new Runnable() {

        @Override
        public void run() {
          for (;;) {
            int c = cnt.getAndSet(0);
            if (c > 0) {
              LOG.info("{}", c);
            }
            try {
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
              break;
            }
          }
        }
      });
    }

    @Override
    public Future<Response> apply(Request request) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("URI: {}", request.uri());
        LOG.debug("Content: {}", request.contentString());
        LOG.debug("Content-Length: {}", request.contentLength().get());
        LOG.debug("Content-Type: {} {}", request.contentType().get(),
            request.contentType().get().equals(Request.ContentTypeJson()));
        LOG.debug("Length: {}", request.length());
        LOG.debug("Params: {}", request.getParams());
        LOG.debug("Chunked: {}", request.isChunked());
        LOG.debug("Remote: {}:{}", request.remoteHost(), request.remotePort());
        for (Iterator<Cookie> it = request.getCookies(); it.hasNext();) {
          Cookie cookie = it.next();
          LOG.debug("Cookie: {}={}", cookie.name(), cookie.value());
        }
        for (Map.Entry<String, String> entry : request.headers()) {
          LOG.debug("Header: {}", entry);
        }
      }

      cnt.incrementAndGet();

      Response response = Response.apply(request.version(), Status.NoContent());
      // response.headerMap().put(Fields.SetCookie(), new Cookie("name", "value"));
      return Future.value(response);
    }
  }

  private TestServer() {
  }

  public static void main(String[] args) {
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.twitter");
    logger.setUseParentHandlers(false);
    logger.addHandler(new SLF4JHandler());

    final ListeningServer server = Httpx.serve(args[0], new HttpServer());

    try {
      Await.result(server.announce(args[1] + "!0"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      LOG.info("Test server start " + Arrays.toString(args));
      Await.ready(server);
    } catch (TimeoutException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
