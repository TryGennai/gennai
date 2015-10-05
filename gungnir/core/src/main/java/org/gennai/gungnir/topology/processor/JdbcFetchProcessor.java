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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolder;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolders;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class JdbcFetchProcessor implements FetchProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(JdbcFetchProcessor.class);

  private static final String DRIVER = "jdbc.fetch.driver";
  private static final String CACHE_SIZE = "jdbc.fetch.cache.size";

  private String sql;
  private String url;
  private String user;
  private String password;
  private Period expire;
  private transient PlaceHolders placeHolders;
  private transient int expireSecs;
  private transient Connection conn;
  private transient PreparedStatement stmt;
  private transient Cache<List<Object>, List<List<Object>>> cache;

  public JdbcFetchProcessor(String url, String user, String password, String sql, Period expire) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.sql = sql;
    this.expire = expire;
  }

  public JdbcFetchProcessor(String url, String user, String password, String sql) {
    this(url, user, password, sql, null);
  }

  @Override
  public GroupFields getGroupFields() {
    if (placeHolders == null) {
      placeHolders = ProcessorUtils.findPlaceHolders(sql);
    }

    if (placeHolders.isEmpty()) {
      return null;
    }
    Set<FieldAccessor> fields = Sets.newLinkedHashSet();
    for (PlaceHolder placeHolder : placeHolders) {
      fields.add(placeHolder.getField());
    }
    return new GroupFields(fields.toArray(new FieldAccessor[0]));
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
  public void open(GungnirConfig config, GungnirContext context) throws ProcessorException {
    placeHolders = ProcessorUtils.findPlaceHolders(sql);
    if (expire != null) {
      expireSecs = expire.toSeconds();
    }

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

    cache = CacheBuilder.newBuilder().maximumSize(config.getInteger(CACHE_SIZE))
        .expireAfterWrite(expireSecs, TimeUnit.SECONDS).build();

    LOG.info("JdbcFetchProcessor opened({})", this);
  }

  private List<List<Object>> execute(GungnirTuple tuple) throws SQLException {
    List<List<Object>> valuesList = Lists.newArrayList();
    ResultSet resultSet = null;

    try {
      int i = 1;
      for (PlaceHolder placeHolder : placeHolders) {
        stmt.setObject(i, placeHolder.getField().getValue(tuple));
        i++;
      }

      resultSet = stmt.executeQuery();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Execute query '{}'", stmt);
      }

      int columnCount = resultSet.getMetaData().getColumnCount();

      while (resultSet.next()) {
        List<Object> values = Lists.newArrayListWithCapacity(columnCount);
        for (int j = 1; j <= columnCount; j++) {
          values.add(resultSet.getObject(j));
        }
        valuesList.add(values);
      }
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          LOG.error("Failed to close resultSet", e);
        }
      }
    }

    return valuesList;
  }

  @Override
  public List<List<Object>> fetch(final GungnirTuple tuple) throws ProcessorException {
    if (stmt == null) {
      throw new ProcessorException("Processor isn't open");
    }

    try {
      List<Object> keyValues = Lists.newArrayList();
      for (PlaceHolder placeHolder : placeHolders) {
        keyValues.add(placeHolder.getField().getValue(tuple));
      }

      List<List<Object>> valuesList = null;
      if (expireSecs > 0) {
        valuesList = cache.get(keyValues, new Callable<List<List<Object>>>() {

          @Override
          public List<List<Object>> call() throws Exception {
            return execute(tuple);
          }
        });
      } else {
        valuesList = execute(tuple);
      }

      return valuesList;
    } catch (ExecutionException e) {
      throw new ProcessorException("Failed to execute query '" + stmt + "'", e);
    } catch (SQLException e) {
      throw new ProcessorException("Failed to execute query '" + stmt + "'", e);
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

    LOG.info("JdbcFetchProcessor closed({})", this);
  }

  @Override
  public String toString() {
    return "jdbc_fetch(" + sql + ")";
  }
}
