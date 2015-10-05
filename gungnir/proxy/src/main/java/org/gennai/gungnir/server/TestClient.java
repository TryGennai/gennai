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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.twitter.finagle.Httpx;
import com.twitter.finagle.Service;
import com.twitter.finagle.httpx.Method;
import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.httpx.Version;
import com.twitter.finagle.util.DefaultTimer;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;

public final class TestClient {

  private TestClient() {
  }

  public static void main(String[] args) {
    final Service<Request, Response> client = Httpx.newService(args[0]);
    final String uri = args[1];
    int parallels = Integer.parseInt(args[2]);
    final int cnt = Integer.parseInt(args[3]);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1024; i++) {
      sb.append('#');
    }
    final String buff = sb.toString();

    ExecutorService executor = Executors.newFixedThreadPool(parallels);
    final CountDownLatch latch = new CountDownLatch(parallels);

    for (int i = 0; i < parallels; i++) {
      executor.submit(new Runnable() {

        @Override
        public void run() {
          Request request = Request.apply(Version.Http11$.MODULE$, Method.Post$.MODULE$, uri);
          request.setContentString(buff);
          request.setContentTypeJson();
          request.contentLength_$eq(request.getLength());
          // request.addCookie(new Cookie("name", "value"));

          for (int i = 0; i < cnt; i++) {
            try {
              Await.ready(client.apply(request).raiseWithin(
                  new Duration(TimeUnit.SECONDS.toNanos(10)),
                  DefaultTimer.twitter()).addEventListener(new FutureEventListener<Response>() {

                @Override
                public void onFailure(Throwable cause) {
                }

                @Override
                public void onSuccess(Response response) {
                }
              }));
            } catch (TimeoutException e) {
              e.printStackTrace();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }

          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    executor.shutdown();
    client.close();
  }
}
