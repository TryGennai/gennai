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

package org.gennai.gungnir.cluster.storm;

import java.util.List;
import java.util.Map;

import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.TopologyInfo;

import com.google.common.collect.Lists;

public class TopologyStats {

  public static final class OutputStats {

    private String streamId;
    private long emitted;

    private OutputStats(String streamId) {
      this.streamId = streamId;
    }

    public String getStreamId() {
      return streamId;
    }

    public long getEmitted() {
      return emitted;
    }
  }

  public static final class InputStats {

    private String componentId;
    private String streamId;
    private long executed;
    private double executeLatency;

    private InputStats(String componentId, String streamId) {
      this.componentId = componentId;
      this.streamId = streamId;
    }

    public String getComponentId() {
      return componentId;
    }

    public String getStreamId() {
      return streamId;
    }

    public long getExecuted() {
      return executed;
    }

    public double getExecuteLatency() {
      if (executed > 0) {
        return executeLatency / executed;
      } else {
        return 0.0;
      }
    }
  }

  public static class ExecutorStats {

    private String id;
    private int uptimeSecs;
    private String host;
    private int port;
    private long emitted;
    private List<OutputStats> outputStats;

    protected ExecutorStats(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public int getUptimeSecs() {
      return uptimeSecs;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public long getEmitted() {
      return emitted;
    }

    public List<OutputStats> getOutputStats() {
      return outputStats;
    }

    private OutputStats getOutputStats(String streamId) {
      if (outputStats == null) {
        outputStats = Lists.newArrayList();
      } else {
        for (OutputStats os : outputStats) {
          if (os.streamId.equals(streamId)) {
            return os;
          }
        }
      }
      OutputStats os = new OutputStats(streamId);
      outputStats.add(os);
      return os;
    }
  }

  public static final class BoltExecutorStats extends ExecutorStats {

    private long executed;
    private double executeLatency;
    private List<InputStats> inputStats;

    private BoltExecutorStats(String id) {
      super(id);
    }

    public long getExecuted() {
      return executed;
    }

    public double getExecuteLatency() {
      if (executed > 0) {
        return executeLatency / executed;
      } else {
        return 0.0;
      }
    }

    public List<InputStats> getInputStats() {
      return inputStats;
    }

    private InputStats getInputStats(GlobalStreamId globalStreamId) {
      if (inputStats == null) {
        inputStats = Lists.newArrayList();
      } else {
        for (InputStats is : inputStats) {
          if (is.componentId.equals(globalStreamId.get_componentId())
              && is.streamId.equals(globalStreamId.get_streamId())) {
            return is;
          }
        }
      }
      InputStats is =
          new InputStats(globalStreamId.get_componentId(), globalStreamId.get_streamId());
      inputStats.add(is);
      return is;
    }
  }

  public static class ComponentStats {

    private String id;
    private long emitted;
    private List<OutputStats> outputStats;
    private List<ExecutorStats> executorStats;

    protected ComponentStats(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public long getEmitted() {
      return emitted;
    }

    public List<OutputStats> getOutputStats() {
      return outputStats;
    }

    public List<ExecutorStats> getExecutorStats() {
      return executorStats;
    }

    private OutputStats getOutputStats(String streamId) {
      if (outputStats == null) {
        outputStats = Lists.newArrayList();
      } else {
        for (OutputStats os : outputStats) {
          if (os.streamId.equals(streamId)) {
            return os;
          }
        }
      }
      OutputStats os = new OutputStats(streamId);
      outputStats.add(os);
      return os;
    }

    protected ExecutorStats getExecutorStats(String executorId) {
      if (executorStats == null) {
        executorStats = Lists.newArrayList();
      } else {
        for (ExecutorStats es : executorStats) {
          if (es.id.equals(executorId)) {
            return es;
          }
        }
      }
      ExecutorStats es = new ExecutorStats(executorId);
      executorStats.add(es);
      return es;
    }
  }

  public static final class BoltStats extends ComponentStats {

    private long executed;
    private double executeLatency;
    private List<InputStats> inputStats;

    private BoltStats(String id) {
      super(id);
    }

    public long getExecuted() {
      return executed;
    }

    public double getExecuteLatency() {
      if (executed > 0) {
        return executeLatency / executed;
      } else {
        return 0.0;
      }
    }

    public List<InputStats> getInputStats() {
      return inputStats;
    }

    private InputStats getInputStats(GlobalStreamId globalStreamId) {
      if (inputStats == null) {
        inputStats = Lists.newArrayList();
      } else {
        for (InputStats is : inputStats) {
          if (is.componentId.equals(globalStreamId.get_componentId())
              && is.streamId.equals(globalStreamId.get_streamId())) {
            return is;
          }
        }
      }
      InputStats is =
          new InputStats(globalStreamId.get_componentId(), globalStreamId.get_streamId());
      inputStats.add(is);
      return is;
    }

    @Override
    protected BoltExecutorStats getExecutorStats(String executorId) {
      if (super.executorStats == null) {
        super.executorStats = Lists.newArrayList();
      } else {
        for (ExecutorStats es : super.executorStats) {
          if (es.id.equals(executorId)) {
            return (BoltExecutorStats) es;
          }
        }
      }
      BoltExecutorStats es = new BoltExecutorStats(executorId);
      super.executorStats.add(es);
      return es;
    }
  }

  public static class WindowStats {

    private String window;
    private List<ComponentStats> componentStats;

    public String getWindow() {
      return window;
    }

    public List<ComponentStats> getComponentStats() {
      return componentStats;
    }

    private ComponentStats getComponentStats(String componentId) {
      if (componentStats == null) {
        componentStats = Lists.newArrayList();
      } else {
        for (ComponentStats cs : componentStats) {
          if (cs.id.equals(componentId)) {
            return cs;
          }
        }
      }
      ComponentStats cs = new ComponentStats(componentId);
      componentStats.add(cs);
      return cs;
    }

    private BoltStats getBlotStats(String componentId) {
      if (componentStats == null) {
        componentStats = Lists.newArrayList();
      } else {
        for (ComponentStats cs : componentStats) {
          if (cs.id.equals(componentId)) {
            return (BoltStats) cs;
          }
        }
      }
      BoltStats cs = new BoltStats(componentId);
      componentStats.add(cs);
      return cs;
    }
  }

  public static class Error {

    private int errorTimeSecs;
    private String host;
    private int port;
    private String error;

    public int getErrorTimeSecs() {
      return errorTimeSecs;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public String getError() {
      return error;
    }
  }

  public static final class ComponentErrors {

    private String id;
    private List<Error> errors;

    private ComponentErrors(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public List<Error> getErrors() {
      return errors;
    }
  }

  private String name;
  private String status;
  private int uptimeSecs;
  private List<WindowStats> stats;
  private List<ComponentErrors> errors;

  public String getName() {
    return name;
  }

  public String getStatus() {
    return status;
  }

  public int getUptimeSecs() {
    return uptimeSecs;
  }

  public List<WindowStats> getStats() {
    return stats;
  }

  public List<ComponentErrors> getErrors() {
    return errors;
  }

  private WindowStats getWindowStats(String window) {
    if (stats == null) {
      stats = Lists.newArrayList();
    } else {
      for (WindowStats ws : stats) {
        if (ws.window.equals(window)) {
          return ws;
        }
      }
    }
    WindowStats ws = new WindowStats();
    ws.window = window;
    stats.add(ws);
    return ws;
  }

  private void addError(String componentId, int errorTimeSecs, String host, int port,
      String error) {
    ComponentErrors ce = null;
    if (errors == null) {
      errors = Lists.newArrayList();
    } else {
      for (ComponentErrors e : errors) {
        if (e.id.equals(componentId)) {
          ce = e;
          break;
        }
      }
    }
    if (ce == null) {
      ce = new ComponentErrors(componentId);
      errors.add(ce);
    }
    if (ce.errors == null) {
      ce.errors = Lists.newArrayList();
    }
    Error e = new Error();
    e.errorTimeSecs = errorTimeSecs;
    e.host = host;
    e.port = port;
    e.error = error;
    ce.errors.add(e);
  }

  public static TopologyStats apply(TopologyInfo topologyInfo, boolean extended) {
    TopologyStats topologyStats = new TopologyStats();
    topologyStats.name = topologyInfo.get_name();
    topologyStats.status = topologyInfo.get_status();
    topologyStats.uptimeSecs = topologyInfo.get_uptime_secs();

    for (ExecutorSummary executorSummary : topologyInfo.get_executors()) {
      int taskStart = executorSummary.get_executor_info().get_task_start();
      int taskEnd = executorSummary.get_executor_info().get_task_end();
      String executorId = taskStart == taskEnd ? String.valueOf(taskStart)
          : taskStart + "-" + taskEnd;

      if (executorSummary.is_set_stats() && !executorSummary.get_component_id().startsWith("__")) {
        for (Map.Entry<String, Map<String, Long>> entry : executorSummary.get_stats().get_emitted()
            .entrySet()) {
          String window = entry.getKey().charAt(0) == ':' ? entry.getKey().substring(1)
              : entry.getKey();
          WindowStats windowStats = topologyStats.getWindowStats(window);
          ComponentStats componentStats;
          if (executorSummary.get_stats().get_specific().is_set_bolt()) {
            componentStats = windowStats.getBlotStats(executorSummary.get_component_id());
          } else {
            componentStats = windowStats.getComponentStats(executorSummary.get_component_id());
          }

          ExecutorStats executorStats = null;
          if (extended) {
            executorStats = componentStats.getExecutorStats(executorId);
            if (executorStats != null) {
              executorStats.uptimeSecs = executorSummary.get_uptime_secs();
              executorStats.host = executorSummary.get_host();
              executorStats.port = executorSummary.get_port();
            }
          }

          for (Map.Entry<String, Long> entry2 : entry.getValue().entrySet()) {
            if (!entry2.getKey().startsWith("__")) {
              componentStats.emitted += entry2.getValue();
              OutputStats outputStats = componentStats.getOutputStats(entry2.getKey());
              outputStats.emitted += entry2.getValue();

              if (executorStats != null) {
                executorStats.emitted += entry2.getValue();
                outputStats = executorStats.getOutputStats(entry2.getKey());
                outputStats.emitted = entry2.getValue();
              }
            }
          }
        }

        if (executorSummary.get_stats().get_specific().is_set_bolt()) {
          Map<String, Map<GlobalStreamId, Long>> executedStats = executorSummary.get_stats()
              .get_specific().get_bolt().get_executed();
          Map<String, Map<GlobalStreamId, Double>> executeLatencyStats =
              executorSummary.get_stats()
                  .get_specific().get_bolt().get_execute_ms_avg();

          for (Map.Entry<String, Map<GlobalStreamId, Long>> entry : executedStats.entrySet()) {
            String window = entry.getKey().charAt(0) == ':' ? entry.getKey().substring(1)
                : entry.getKey();
            WindowStats windowStats = topologyStats.getWindowStats(window);
            BoltStats boltStats =
                (BoltStats) windowStats.getComponentStats(executorSummary.get_component_id());
            BoltExecutorStats executorStats = null;
            if (extended) {
              executorStats = boltStats.getExecutorStats(executorId);
            }

            for (Map.Entry<GlobalStreamId, Long> entry2 : entry.getValue().entrySet()) {
              boltStats.executed += entry2.getValue();
              InputStats inputStats = boltStats.getInputStats(entry2.getKey());
              inputStats.executed += entry2.getValue();

              if (executorStats != null) {
                executorStats.executed += entry2.getValue();
                inputStats = executorStats.getInputStats(entry2.getKey());
                inputStats.executed = entry2.getValue();
              }
            }
          }

          for (Map.Entry<String, Map<GlobalStreamId, Double>> entry : executeLatencyStats
              .entrySet()) {
            String window = entry.getKey().charAt(0) == ':' ? entry.getKey().substring(1)
                : entry.getKey();
            WindowStats windowStats = topologyStats.getWindowStats(window);
            BoltStats boltStats =
                (BoltStats) windowStats.getComponentStats(executorSummary.get_component_id());
            BoltExecutorStats executorStats = null;
            if (extended) {
              executorStats = boltStats.getExecutorStats(executorId);
            }

            for (Map.Entry<GlobalStreamId, Double> entry2 : entry.getValue().entrySet()) {
              long executed = executedStats.get(entry.getKey()).get(entry2.getKey());

              boltStats.executeLatency += entry2.getValue() * executed;
              InputStats inputStats = boltStats.getInputStats(entry2.getKey());
              inputStats.executeLatency += entry2.getValue() * executed;

              if (executorStats != null) {
                executorStats.executeLatency += entry2.getValue() * executed;
                inputStats = executorStats.getInputStats(entry2.getKey());
                inputStats.executeLatency = entry2.getValue() * executed;
              }
            }
          }
        }
      }
    }

    for (Map.Entry<String, List<ErrorInfo>> entry : topologyInfo.get_errors().entrySet()) {
      for (ErrorInfo errorInfo : entry.getValue()) {
        topologyStats.addError(entry.getKey(), errorInfo.get_error_time_secs(),
            errorInfo.get_host(), errorInfo.get_port(), errorInfo.get_error());
      }
    }

    return topologyStats;
  }
}
