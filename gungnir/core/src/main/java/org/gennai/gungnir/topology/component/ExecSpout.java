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

package org.gennai.gungnir.topology.component;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.dispatcher.BaseDispatcher;
import org.gennai.gungnir.topology.dispatcher.Dispatcher;
import org.gennai.gungnir.topology.dispatcher.FilterDispatcher;
import org.gennai.gungnir.topology.dispatcher.MultiDispatcher;
import org.gennai.gungnir.topology.operator.PartitionOperator;
import org.gennai.gungnir.topology.operator.SpoutOperator;
import org.gennai.gungnir.topology.operator.metrics.Metrics;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.utils.SnapshotTimer;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ExecSpout extends BaseRichSpout implements GungnirComponent {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(ExecSpout.class);

  private static final String SPOUT_NAME = "EXEC_SPOUT";

  private GungnirContext context;
  private SpoutOperator incomingOperator;
  private List<PartitionOperator> outgoingOperators;
  private TopologyContext topologyContext;
  private SnapshotTimer snapshotTimer;
  private Map<String, Metrics> metricsMap;

  private static class SpoutDispatcher extends BaseDispatcher {

    private static final long serialVersionUID = SERIAL_VERSION_UID;

    private SpoutOutputCollector collector;

    @Override
    protected void prepare() {
    }

    @Override
    public void dispatch(TupleValues tupleValues) {
      collector.emit(getSource().getName(), new Values(tupleValues));

      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatch {} from {}", tupleValues, getSource().getName());
      }
    }

    @Override
    public String toString() {
      return "-SpoutDispatcher(" + getSource().getName() + ")->>";
    }
  }

  public ExecSpout(GungnirContext context) {
    this.context = context;
  }

  @Override
  public String getName() {
    return SPOUT_NAME;
  }

  @Override
  public TopologyContext getTopologyContext() {
    return topologyContext;
  }

  @Override
  public SnapshotTimer getShapshotTimer() {
    return snapshotTimer;
  }

  public void setIncomingOperator(SpoutOperator incomingOperator) {
    this.incomingOperator = incomingOperator;
  }

  public SpoutOperator getIncomingOperator() {
    return incomingOperator;
  }

  public List<PartitionOperator> getOutgoingOperators() {
    return outgoingOperators;
  }

  public void addOutgoingOperators(List<PartitionOperator> outgoingOperators) {
    if (this.outgoingOperators == null) {
      this.outgoingOperators = Lists.newArrayList();
    }
    this.outgoingOperators.addAll(outgoingOperators);
  }

  public void registerMetrics(Map<String, Metrics> metrics) {
    if (metrics != null && metrics.size() > 0) {
      if (metricsMap == null) {
        metricsMap = Maps.newHashMap();
      }
      metricsMap.putAll(metrics);
    }
  }

  @Override
  public void open(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
      SpoutOutputCollector collector) {
    LOG.info("open({}[{}]) TaskId: {}, ThisComponetTasks: {}, ThisWorkerTasks: {}",
        context.getThisComponentId(), context.getThisTaskIndex(), context.getThisTaskId(),
        context.getComponentTasks(context.getThisComponentId()), context.getThisWorkerTasks());

    @SuppressWarnings("unchecked")
    GungnirConfig config = GungnirConfig.wrap((Map<String, Object>) stormConf.get(GUNGNIR_CONFIG));

    topologyContext = context;

    if (config.getBoolean(TOPOLOGY_METRICS_ENABLED) && metricsMap != null) {
      for (Map.Entry<String, Metrics> entry : metricsMap.entrySet()) {
        topologyContext.registerMetric(entry.getKey(), entry.getValue(),
            config.getInteger(TOPOLOGY_METRICS_INTERVAL_SECS));
      }
    }

    this.context.setComponent(this);

    incomingOperator.doPrepare(config, this.context);

    for (PartitionOperator partitionOperator : outgoingOperators) {
      SpoutDispatcher spoutDispatcher = new SpoutDispatcher();
      spoutDispatcher.collector = collector;

      Dispatcher dispatcher = partitionOperator.getDispatcher();
      if (dispatcher == null) {
        partitionOperator.setDispatcher(spoutDispatcher);
      } else if (dispatcher instanceof MultiDispatcher) {
        for (Dispatcher d : ((MultiDispatcher) dispatcher).getDispatchers()) {
          if (d instanceof FilterDispatcher) {
            ((FilterDispatcher) d).setDispatcher(spoutDispatcher);
          }
        }
      } else if (dispatcher instanceof FilterDispatcher) {
        ((FilterDispatcher) dispatcher).setDispatcher(spoutDispatcher);
      }
    }

    // TODO parallelization
    // snapshotTimer = new SnapshotTimer(config.getInteger(COMPONENT_SNAPSHOT_QUEUE_SIZE),
    //     config.getInteger(COMPONENT_SNAPSHOT_PARALLELISM));
    snapshotTimer = new SnapshotTimer(getName() + "_" + topologyContext.getThisTaskIndex());
  }

  @Override
  public void nextTuple() {
    incomingOperator.nextTuple();

    if (LOG.isDebugEnabled()) {
      LOG.debug("nextTuple({} {}[{}])", context.getTopologyId(), getName(),
          this.getTopologyContext().getThisTaskIndex());
    }
  }

  @Override
  public void close() {
    incomingOperator.doCleanup();
    try {
      snapshotTimer.stop();
    } catch (SchedulerException e) {
      LOG.error("Failed to stop snapshot timer", e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    for (PartitionOperator partitionOperator : outgoingOperators) {
      declarer.declareStream(partitionOperator.getName(), new Fields(TUPLE_FIELD));
    }
  }
}
