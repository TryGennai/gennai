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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.log.DebugLogger;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.dispatcher.Dispatcher;
import org.gennai.gungnir.topology.operator.metrics.Metrics;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.TupleValues;

import com.google.common.collect.Maps;

public abstract class BaseOperator implements Operator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private OperatorContext operatorContext;
  private int parallelism;
  private Dispatcher dispatcher;
  private Map<String, Metrics> metricsMap;

  private GungnirConfig config;
  private GungnirContext context;
  private DebugLogger debugLogger;

  private transient String description;
  private transient boolean prepared = false;
  private transient boolean cleanedup = false;

  public BaseOperator() {
    operatorContext = new OperatorContext(this);
  }

  protected BaseOperator(BaseOperator c) {
    this.operatorContext = c.operatorContext;
    this.metricsMap = c.metricsMap;
  }

  @Override
  public void setId(int id) {
    operatorContext.setId(id);
  }

  @Override
  public int getId() {
    return operatorContext.getId();
  }

  @Override
  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public String getName() {
    return operatorContext.getName() + "_" + operatorContext.getId();
  }

  @Override
  public List<Field> getOutputFields() throws GungnirTopologyException {
    return null;
  }

  @Override
  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.dispatcher.setSource(this);
  }

  @Override
  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public void registerMetrics(String name, Metrics metrics) {
    if (metricsMap == null) {
      metricsMap = Maps.newHashMap();
    }
    metricsMap.put(name, metrics);
  }

  @Override
  public Map<String, Metrics> getMetrics() {
    return metricsMap;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Metrics> T getMetrics(String name) {
    if (metricsMap != null) {
      return (T) metricsMap.get(name);
    } else {
      return null;
    }
  }

  protected OperatorContext getOperatorContext() {
    return operatorContext;
  }

  protected GungnirConfig getConfig() {
    return config;
  }

  protected GungnirContext getContext() {
    return context;
  }

  protected DebugLogger getMonitorLogger() {
    return debugLogger;
  }

  @Override
  public boolean isPrepared() {
    return prepared;
  }

  @Override
  public boolean isCleanedup() {
    return cleanedup;
  }

  protected abstract void prepare();

  @Override
  public void doPrepare(GungnirConfig config, GungnirContext context) {
    this.config = config;
    this.context = context;
    debugLogger = context.getDebugLogger(config);

    prepare();
    this.prepared = true;
  }

  protected void dispatch(TupleValues tupleValues) {
    if (dispatcher != null) {
      if (!dispatcher.isPrepared()) {
        dispatcher.doPrepare(config, context);
      }
      dispatcher.dispatch(tupleValues);
    }
  }

  protected void cleanup() {
  }

  @Override
  public void doCleanup() {
    if (dispatcher != null && dispatcher.isPrepared()) {
      dispatcher.doCleanup();
    }
    if (prepared && !cleanedup) {
      cleanup();
      cleanedup = true;
    }
  }

  @Override
  public String toString() {
    if (description == null) {
      Description desc = this.getClass().getAnnotation(Description.class);
      if (desc == null) {
        return getName();
      }

      StringBuilder sb = new StringBuilder();
      try {
        if (desc.parameterNames().length > 0) {
          int paramCount = 0;
          for (String parameterName : desc.parameterNames()) {
            java.lang.reflect.Field field = null;
            try {
              field = this.getClass().getDeclaredField(parameterName);
            } catch (NoSuchFieldException e) {
              field = this.getClass().getSuperclass().getDeclaredField(parameterName);
            }
            field.setAccessible(true);
            Object arg = field.get(this);
            if (arg != null) {
              if (paramCount > 0) {
                sb.append(", ");
              }
              paramCount++;
              if (arg.getClass().isArray()) {
                sb.append(Arrays.toString((Object[]) arg));
              } else {
                sb.append(arg.toString());
              }
            }
          }
        }
      } catch (Exception e) {
        sb.append(e);
      }

      description = sb.toString();
    }

    return getName() + '(' + description + ')';
  }
}
