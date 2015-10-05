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

package org.gennai.gungnir.topology.processor;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolder;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolders;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.json.StructSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Maps;

public class JdbcPersistProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(JdbcPersistProcessor.class);

  private static final String DRIVER = "jdbc.persist.driver";

  private String url;
  private String user;
  private String password;
  private String sql;
  private transient Connection conn;
  private transient PreparedStatement stmt;
  private transient Map<String, Integer[]> fieldIndexesMap;
  private transient ObjectMapper mapper;

  public JdbcPersistProcessor(String url, String user, String password, String sql) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.sql = sql;
  }

  private static String replacePlaceHolders(PlaceHolders placeHolders) {
    if (placeHolders.isEmpty()) {
      return placeHolders.getSrc();
    }

    StringBuilder sb = new StringBuilder();
    int start = 0;
    for (PlaceHolder placeHolder : placeHolders) {
      sb.append(placeHolders.getSrc().substring(start, placeHolder.getStart()));
      sb.append('?');
      start = placeHolder.getEnd();
    }
    sb.append(placeHolders.getSrc().substring(start));

    return sb.toString();
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    PlaceHolders placeHolders = null;
    try {
      Class.forName(config.getString(DRIVER));
      conn = DriverManager.getConnection(url, user, password);

      placeHolders = ProcessorUtils.findPlaceHolders(sql);
      stmt = conn.prepareStatement(replacePlaceHolders(placeHolders));
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to load driver", e);
    } catch (SQLException e) {
      LOG.error("Failed to open statement", e);
    }

    fieldIndexesMap = Maps.newHashMap();
    for (Map.Entry<String, List<String>> entry : outputFieldNames.entrySet()) {
      Integer[] fieldIndexes = new Integer[entry.getValue().size()];
      for (int i = 0; i < entry.getValue().size(); i++) {
        int j = 0;
        for (PlaceHolder placeHolder : placeHolders) {
          if (placeHolder.getField().getFieldName().equals(entry.getValue().get(i))) {
            fieldIndexes[i] = j;
            break;
          }
          j++;
        }
      }
      fieldIndexesMap.put(entry.getKey(), fieldIndexes);
    }

    SimpleModule module = new SimpleModule("GungnirModule",
        new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null, null, null));
    module.addSerializer(Struct.class, new StructSerializer());

    mapper = new ObjectMapper();
    mapper.registerModule(module);

    LOG.info("JdbcPersistProcessor opened({})", this);
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    if (stmt == null) {
      throw new ProcessorException("Processor isn't open");
    }

    try {
      for (TupleValues tupleValues : tuples) {
        Integer[] fieldIndexes = fieldIndexesMap.get(tupleValues.getTupleName());
        for (int i = 0; i < fieldIndexes.length; i++) {
          Object value = tupleValues.getValues().get(fieldIndexes[i]);
          if (value instanceof List) {
            stmt.setObject(i + 1, mapper.writeValueAsString(value));
          } else if (value instanceof Map) {
            stmt.setObject(i + 1, mapper.writeValueAsString(value));
          } else if (value instanceof Struct) {
            stmt.setObject(i + 1, mapper.writeValueAsString(value));
          } else {
            stmt.setObject(i + 1, value);
          }
        }
        stmt.addBatch();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Execute batch '{}'", stmt);
        }
      }

      stmt.executeBatch();
    } catch (SQLException e) {
      LOG.error("Failed to execute batch", e);
    } catch (JsonGenerationException e) {
      throw new ProcessorException("Failed to convert json format", e);
    } catch (JsonMappingException e) {
      throw new ProcessorException("Failed to convert json format", e);
    } catch (IOException e) {
      throw new ProcessorException("Failed to convert json format", e);
    }
  }

  @Override
  public void close() {
    try {
      if (stmt != null) {
        stmt.close();
      }

      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      LOG.error("Failed to close statement", e);
    }

    LOG.info("JdbcPersistProcessor closed({})", this);
  }

  @Override
  public String toString() {
    return "jdbc_persist(" + sql + ")";
  }
}
