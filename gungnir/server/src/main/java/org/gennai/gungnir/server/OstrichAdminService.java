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

import java.util.concurrent.TimeUnit;

import org.apache.storm.guava.collect.Lists;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map$;
import scala.util.matching.Regex;

import com.twitter.ostrich.admin.AdminHttpService;
import com.twitter.ostrich.admin.AdminServiceFactory;
import com.twitter.ostrich.admin.CustomHttpHandler;
import com.twitter.ostrich.admin.RuntimeEnvironment;
import com.twitter.ostrich.admin.StatsFactory;
import com.twitter.util.Duration;

public class OstrichAdminService {

  private int port;
  private int backlog;
  private AdminHttpService delegate;

  public OstrichAdminService(int port, int backlog) {
    this.port = port;
    this.backlog = backlog;
  }

  public void start() {
    List<Duration> defaultLatchIntervals = JavaConverters.asScalaBufferConverter(
        Lists.<Duration>newArrayList(Duration.apply(1, TimeUnit.MINUTES))).asScala().toList();

    delegate = new AdminServiceFactory(
        port,
        backlog,
        List$.MODULE$.<StatsFactory>empty(),
        Option.<String>empty(),
        List$.MODULE$.<Regex>empty(),
        Map$.MODULE$.<String, CustomHttpHandler>empty(),
        defaultLatchIntervals)
        .apply(new RuntimeEnvironment(this));
  }

  public void shutdown() {
    if (delegate != null) {
      delegate.shutdown();
    }
  }
}
