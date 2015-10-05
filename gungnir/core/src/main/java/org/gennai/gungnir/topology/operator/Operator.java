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

package org.gennai.gungnir.topology.operator;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.dispatcher.Dispatcher;
import org.gennai.gungnir.topology.operator.metrics.Metrics;
import org.gennai.gungnir.tuple.Field;

public interface Operator extends Serializable, Cloneable {

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Description {

    String name();

    String[] parameterNames() default {};
  }

  void setId(int id);

  int getId();

  void setParallelism(int parallelism);

  int getParallelism();

  String getName();

  List<Field> getOutputFields() throws GungnirTopologyException;

  void setDispatcher(Dispatcher dispatcher);

  Dispatcher getDispatcher();

  void registerMetrics(String name, Metrics metrics);

  Map<String, Metrics> getMetrics();

  <T extends Metrics> T getMetrics(String name);

  boolean isPrepared();

  boolean isCleanedup();

  void doPrepare(GungnirConfig config, GungnirContext context);

  void doCleanup();
}
