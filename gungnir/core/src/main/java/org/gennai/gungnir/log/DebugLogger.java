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

package org.gennai.gungnir.log;

import org.gennai.gungnir.topology.operator.Operator;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.Marker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DebugLogger {

  private Logger logger;
  private static ObjectMapper MAPPER = new ObjectMapper();

  public DebugLogger() {
  }

  public DebugLogger(Logger logger) {
    this.logger = logger;
  }

  public void logging(String msg) {
    if (logger != null && logger.isInfoEnabled()) {
      logger.info(msg);
    }
  }

  public void logging(Marker marker, String msg) {
    if (logger != null && logger.isInfoEnabled()) {
      logger.info(marker, msg);
    }
  }

  public void logging(String format, Object... arguments) {
    if (logger != null && logger.isInfoEnabled()) {
      logger.info(format, arguments);
    }
  }

  public void logging(Marker marker, String format, Object... arguments) {
    if (logger != null && logger.isInfoEnabled()) {
      logger.info(marker, format, arguments);
    }
  }

  public void logging(Marker marker, Operator source, Operator target, GungnirTuple tuple) {
    if (logger != null && logger.isInfoEnabled()) {
      ObjectNode logNode = MAPPER.createObjectNode();
      logNode.put("source", source.getName());
      logNode.put("target", target.getName());
      logNode.putPOJO("tuple", tuple.getTupleValues());
      try {
        logging(marker, MAPPER.writeValueAsString(logNode));
      } catch (Exception e) {
        logger.error("Failed to convert json format", e);
      }
    }
  }
}
