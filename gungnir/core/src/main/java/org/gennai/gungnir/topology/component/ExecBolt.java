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
import java.util.Set;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.dispatcher.BaseDispatcher;
import org.gennai.gungnir.topology.dispatcher.Dispatcher;
import org.gennai.gungnir.topology.dispatcher.FilterDispatcher;
import org.gennai.gungnir.topology.dispatcher.MultiDispatcher;
import org.gennai.gungnir.topology.operator.PartitionOperator;
import org.gennai.gungnir.topology.operator.metrics.Metrics;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.utils.SnapshotTimer;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ExecBolt extends BaseRichBolt implements GungnirComponent {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(ExecBolt.class);

  private static final String BOLT_NAME = "EXEC_BOLT_";

  private GungnirContext context;
  private int id;
  private Map<String, PartitionOperator> incomingOperatorsMap;
  private List<PartitionOperator> outgoingOperators;
  private Map<String, Metrics> metricsMap;
  private TopologyContext topologyContext;
  private SnapshotTimer snapshotTimer;

  private static class BoltDispatcher extends BaseDispatcher {

    private static final long serialVersionUID = SERIAL_VERSION_UID;

    private OutputCollector collector;

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
      return "-BoltDispatcher(" + getSource().getName() + ")->>";
    }
  }

  public ExecBolt(GungnirContext context) {
    this.context = context;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return BOLT_NAME + id;
  }

  public List<PartitionOperator> getOutgoingOperators() {
    return outgoingOperators;
  }

  @Override
  public TopologyContext getTopologyContext() {
    return topologyContext;
  }

  @Override
  public SnapshotTimer getShapshotTimer() {
    return snapshotTimer;
  }

  public void addIncomingOperator(PartitionOperator incomingOperator) {
    if (this.incomingOperatorsMap == null) {
      this.incomingOperatorsMap = Maps.newHashMap();
    }
    this.incomingOperatorsMap.put(incomingOperator.getName(), incomingOperator);
  }

  public void addOutgoingOperators(List<PartitionOperator> outgoingOperators) {
    if (this.outgoingOperators == null) {
      this.outgoingOperators = Lists.newArrayList();
    }
    Set<String> names = Sets.newHashSet();
    for (PartitionOperator outgoingOperator : this.outgoingOperators) {
      names.add(outgoingOperator.getName());
    }
    for (PartitionOperator operator : outgoingOperators) {
      if (!names.contains(operator.getName())) {
        this.outgoingOperators.add(operator);
      }
    }
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
  public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
      OutputCollector collector) {
    LOG.info(
        "ComponentId: {}, TaskIndex: {}, TaskId: {}, ThisComponetTasks: {}, ThisWorkerTasks: {}",
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

    for (PartitionOperator partitionOperator : incomingOperatorsMap.values()) {
      partitionOperator.doPrepare(config, this.context);
    }

    for (PartitionOperator partitionOperator : outgoingOperators) {
      BoltDispatcher boltDispatcher = new BoltDispatcher();
      boltDispatcher.collector = collector;

      Dispatcher dispatcher = partitionOperator.getDispatcher();
      if (dispatcher == null) {
        partitionOperator.setDispatcher(boltDispatcher);
      } else if (dispatcher instanceof MultiDispatcher) {
        for (Dispatcher d : ((MultiDispatcher) dispatcher).getDispatchers()) {
          if (d instanceof FilterDispatcher) {
            ((FilterDispatcher) d).setDispatcher(boltDispatcher);
          }
        }
      } else if (dispatcher instanceof FilterDispatcher) {
        ((FilterDispatcher) dispatcher).setDispatcher(boltDispatcher);
      }
    }

    // TODO parallelization
    // snapshotTimer = new SnapshotTimer(config.getInteger(COMPONENT_SNAPSHOT_QUEUE_SIZE),
    //     config.getInteger(COMPONENT_SNAPSHOT_PARALLELISM));
    snapshotTimer = new SnapshotTimer(getName() + "_" + topologyContext.getThisTaskIndex());
  }

  @Override
  public void execute(Tuple input) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}[{}]) {}", context.getTopologyId(), getName(),
          this.getTopologyContext().getThisTaskIndex(), input);
    }

    PartitionOperator operator = incomingOperatorsMap.get(input.getSourceStreamId());

    TupleValues tupleValues = (TupleValues) input.getValueByField(TUPLE_FIELD);
    GungnirTuple tuple = new GungnirTuple(
        context.getOutputFields().get(operator.getName()).get(tupleValues.getTupleName()),
        tupleValues);
    operator.execute(tuple);
  }

  @Override
  public void cleanup() {
    for (PartitionOperator partitionOperator : incomingOperatorsMap.values()) {
      partitionOperator.doCleanup();
    }
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
