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

package org.gennai.gungnir.topology;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.log.DebugLogger;
import org.gennai.gungnir.topology.component.GungnirComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.SocketAppender;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;

public class GungnirContext implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(GungnirContext.class);

  private static final String TOPOLOGY_ID = "TOPOLOGY_ID";
  private static final String ACCOUNT_ID = "ACCOUNT_ID";
  private static final String TASK_INDEX = "TASK_INDEX";
  private static final String TASK_ID = "TASK_ID";
  private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{(.+?)\\}");

  private String topologyId;
  private String topologyName;
  private String accountId;
  private Map<String, Map<String, List<String>>> outputFields;
  private Map<String, GroupFields> groupFields;
  private GungnirComponent component;
  private static DebugLogger DEBUG_LOG;

  public void setTopologyId(String topologyId) {
    this.topologyId = topologyId;
  }

  public String getTopologyId() {
    return topologyId;
  }

  public void setTopologyName(String topologyName) {
    this.topologyName = topologyName;
  }

  public String getTopologyName() {
    return topologyName;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public String getAccountId() {
    return accountId;
  }

  public void setOutputFields(Map<String, Map<String, List<String>>> outputFields) {
    this.outputFields = outputFields;
  }

  public Map<String, Map<String, List<String>>> getOutputFields() {
    return outputFields;
  }

  public void setGroupFields(Map<String, GroupFields> groupFields) {
    this.groupFields = groupFields;
  }

  public Map<String, GroupFields> getGroupFields() {
    return groupFields;
  }

  public synchronized DebugLogger getDebugLogger(GungnirConfig config) {
    if (DEBUG_LOG == null) {
      if (config.getBoolean(DEBUG_ENABLED)) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        String host = config.getString(LOG_RECEIVER_HOST);
        if (host != null) {
          SocketAppender appender = new SocketAppender();
          appender.setRemoteHost(host);
          appender.setPort(config.getInteger(LOG_RECEIVER_PORT));
          appender.setQueueSize(config.getInteger(DEBUG_LOG_APPEND_QUEUE_SIZE));
          appender.setReconnectionDelay(config.getInteger(DEBUG_LOG_APPEND_RECONNECT_DELAY));
          appender.setContext(lc);
          appender.getStatusManager().add(new StatusListener() {

            @Override
            public void addStatusEvent(Status status) {
              LOG.info(status.getMessage());
            }
          });
          appender.start();

          ch.qos.logback.classic.Logger logger = lc.getLogger(LOGGER_NAME);
          logger.addAppender(appender);
          logger.setLevel(Level.INFO);
          DEBUG_LOG = new DebugLogger(logger);
        } else {
          if (lc.exists(LOGGER_NAME) != null) {
            DEBUG_LOG = new DebugLogger(LoggerFactory.getLogger(LOGGER_NAME));
          } else {
            DEBUG_LOG = new DebugLogger();
          }
        }
      } else {
        DEBUG_LOG = new DebugLogger();
      }
    }

    return DEBUG_LOG;
  }

  public void setComponent(GungnirComponent component) {
    this.component = component;
  }

  public GungnirComponent getComponent() {
    return component;
  }

  public Object get(String name) {
    if (name.equalsIgnoreCase(TOPOLOGY_ID)) {
      return topologyId;
    } else if (name.equalsIgnoreCase(ACCOUNT_ID)) {
      return accountId;
    } else if (name.equalsIgnoreCase(TASK_INDEX)) {
      return component.getTopologyContext().getThisTaskIndex();
    } else if (name.equalsIgnoreCase(TASK_ID)) {
      return component.getTopologyContext().getThisTaskId();
    }
    return null;
  }

  public String replaceVariable(String src) {
    StringBuilder sb = new StringBuilder();
    int start = 0;
    Matcher matcher = VARIABLE_PATTERN.matcher(src);
    while (matcher.find()) {
      sb.append(src.substring(start, matcher.start()));
      start = matcher.end();
      String name = matcher.group(1);
      Object value = get(name);
      if (value != null) {
        sb.append(value.toString());
      } else {
        sb.append(matcher.group(0));
      }
    }
    if (start == 0) {
      return src;
    }
    if (start <= src.length() - 1) {
      sb.append(src.substring(start));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "{topologyId=" + topologyId + ", topologyName=" + topologyName + ", accountId="
        + accountId + ", outputFields=" + outputFields + "}";
  }
}
