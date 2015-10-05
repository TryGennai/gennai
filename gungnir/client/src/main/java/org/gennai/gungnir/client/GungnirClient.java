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

package org.gennai.gungnir.client;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.thrift.ErrorCode;
import org.gennai.gungnir.thrift.GungnirServerException;
import org.gennai.gungnir.thrift.GungnirService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.ChannelWriteException;
import com.twitter.finagle.FailedFastException;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.util.DefaultTimer;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.TimeoutException;

public final class GungnirClient {

  private static final Logger LOG = LoggerFactory.getLogger(GungnirClient.class);

  private GungnirClient() {
  }

  public static class Connection {

    private GungnirService.ServiceIface client;
    private GungnirConfig config;
    private String sessionId;

    // Accessed by node-java
    // CHECKSTYLE IGNORE VisibilityModifier FOR NEXT 1 LINES
    public ExceptionInfo lastError;

    public Connection(GungnirService.ServiceIface client, GungnirConfig config, String sessionId)
        throws GungnirClientException {
      this.client = client;
      this.config = config;
      this.sessionId = sessionId;
    }

    public Statement createStatement() throws GungnirServerException, GungnirClientException {
      lastError = null;
      try {
        String statementId = Await.result(client.createStatement(sessionId).raiseWithin(
            new Duration(TimeUnit.MILLISECONDS.toNanos(config
                .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));
        return new Statement(client, config, statementId);
      } catch (GungnirServerException e) {
        lastError = new ExceptionInfo(e.code, e.message);
        throw e;
      } catch (TimeoutException e) {
        LOG.warn("Timed out");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Timed out");
      } catch (ChannelWriteException e) {
        LOG.warn("Channel has been disconnected");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Channel has been disconnected");
      } catch (FailedFastException e) {
        LOG.warn("Channel has been disconnected");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Channel has been disconnected");
      } catch (NoBrokersAvailableException e) {
        LOG.warn("There is no available brokers");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("There is no available brokers");
      } catch (Exception e) {
        LOG.error("Failed to create statement", e);
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Failed to create statement", e);
      }
    }

    public void close() {
      try {
        Await.result(client.closeConnection(sessionId).raiseWithin(
            new Duration(TimeUnit.MILLISECONDS.toNanos(config
                .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));
      } catch (GungnirServerException e) {
        LOG.warn("Failed to close connection", e);
      } catch (TimeoutException e) {
        LOG.warn("Timed out");
      } catch (ChannelWriteException e) {
        LOG.warn("Channel has been disconnected");
      } catch (FailedFastException e) {
        LOG.warn("Channel has been disconnected");
      } catch (NoBrokersAvailableException e) {
        LOG.warn("There is no available brokers");
      } catch (Exception e) {
        LOG.error("Failed to close connection", e);
      }
    }

    @Override
    public String toString() {
      return "Connection(" + sessionId + ")";
    }
  }

  public static class Statement {

    private GungnirService.ServiceIface client;
    private GungnirConfig config;

    // Accessed by node-java
    // CHECKSTYLE IGNORE VisibilityModifier FOR NEXT 2 LINES
    public String statementId;
    public ExceptionInfo lastError;

    Statement(GungnirService.ServiceIface client, GungnirConfig config, String statementId) {
      this.client = client;
      this.config = config;
      this.statementId = statementId;
    }

    public String execute(String command) throws GungnirServerException, GungnirClientException {
      lastError = null;
      try {
        return Await.result(client.execute(statementId, command).raiseWithin(
            new Duration(TimeUnit.MILLISECONDS.toNanos(config
                .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));
      } catch (GungnirServerException e) {
        lastError = new ExceptionInfo(e.code, e.message);
        throw e;
      } catch (TimeoutException e) {
        LOG.warn("Timed out");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Timed out");
      } catch (ChannelWriteException e) {
        LOG.warn("Channel has been disconnected");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Channel has been disconnected");
      } catch (FailedFastException e) {
        LOG.warn("Channel has been disconnected");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Channel has been disconnected");
      } catch (NoBrokersAvailableException e) {
        LOG.warn("There is no available brokers");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("There is no available brokers");
      } catch (Exception e) {
        LOG.error("Failed to execute command", e);
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Failed to execute command", e);
      }
    }

    public void uploadFile(Path path) throws GungnirServerException,
        GungnirClientException {
      lastError = null;
      try {
        Await.result(client.beginFileUpload(statementId, path.toFile().getName()).raiseWithin(
            new Duration(TimeUnit.MILLISECONDS.toNanos(config
                .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));


        SeekableByteChannel sbc = null;
        int fileSize = 0;
        CRC32 crc32 = new CRC32();

        try {
          sbc = Files.newByteChannel(path, StandardOpenOption.READ);
          ByteBuffer buff = ByteBuffer.allocate(8192);
          int size = -1;
          while ((size = sbc.read(buff)) != -1) {
            fileSize += size;
            crc32.update(buff.array(), 0, size);

            buff.flip();

            Await.result(client.uploadChunk(statementId, buff).raiseWithin(
                new Duration(TimeUnit.MILLISECONDS.toNanos(config
                    .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));

            buff = ByteBuffer.allocate(8192);
          }
        } finally {
          if (sbc != null) {
            sbc.close();
          }
        }

        Await.result(client.finishFileUpload(statementId, fileSize, crc32.getValue()).raiseWithin(
            new Duration(TimeUnit.MILLISECONDS.toNanos(config
                .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));
      } catch (GungnirServerException e) {
        lastError = new ExceptionInfo(e.code, e.message);
        throw e;
      } catch (TimeoutException e) {
        LOG.warn("Timed out");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Timed out");
      } catch (ChannelWriteException e) {
        LOG.warn("Channel has been disconnected");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Channel has been disconnected");
      } catch (FailedFastException e) {
        LOG.warn("Channel has been disconnected");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Channel has been disconnected");
      } catch (NoBrokersAvailableException e) {
        LOG.warn("There is no available brokers");
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("There is no available brokers");
      } catch (Exception e) {
        LOG.error("Failed to execute command", e);
        lastError = new ExceptionInfo(ErrorCode.ERROR_INTERNAL_ERROR, e.getMessage());
        throw new GungnirClientException("Failed to execute command", e);
      }
    }

    public void close() {
      try {
        Await.result(client.closeStatement(statementId).raiseWithin(
            new Duration(TimeUnit.MILLISECONDS.toNanos(config
                .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));
      } catch (GungnirServerException e) {
        LOG.warn("Failed to close statement", e);
      } catch (TimeoutException e) {
        LOG.warn("Timed out");
      } catch (ChannelWriteException e) {
        LOG.warn("Channel has been disconnected");
      } catch (FailedFastException e) {
        LOG.warn("Channel has been disconnected");
      } catch (NoBrokersAvailableException e) {
        LOG.warn("There is no available brokers");
      } catch (Exception e) {
        LOG.error("Failed to close statement", e);
      }
    }

    @Override
    public String toString() {
      return "Statement(" + statementId + ")";
    }
  }

  public static Connection getConnection(GungnirConfig config, String userName, String password)
      throws GungnirServerException, GungnirClientException {
    String dest = null;
    if (config.getString(CLUSTER_MODE).equals(LOCAL_CLUSTER)) {
      dest = config.getString(GUNGNIR_SERVER_HOST) + ':' + config.getInteger(GUNGNIR_SERVER_PORT);
    } else {
      List<String> zkServers = config.getList(CLUSTER_ZOOKEEPER_SERVERS);
      dest = "zk!" + StringUtils.join(zkServers, ",") + "!" + config.getString(GUNGNIR_NODE_PATH)
          + SERVERS_NODE_PATH;
    }
    GungnirService.ServiceIface client = Thrift.newIface(dest, GungnirService.ServiceIface.class);

    try {
      String sessionId = Await.result(client.createConnection(userName, password).raiseWithin(
          new Duration(TimeUnit.MILLISECONDS.toNanos(config
              .getInteger(GUNGNIR_CLIENT_RESPONSE_TIMEOUT))), DefaultTimer.twitter()));
      return new Connection(client, config, sessionId);
    } catch (GungnirServerException e) {
      throw e;
    } catch (TimeoutException e) {
      LOG.warn("Timed out");
      throw new GungnirClientException("Timed out");
    } catch (ChannelWriteException e) {
      LOG.warn("Channel has been disconnected");
      throw new GungnirClientException("Channel has been disconnected");
    } catch (FailedFastException e) {
      LOG.warn("Channel has been disconnected");
      throw new GungnirClientException("Channel has been disconnected");
    } catch (NoBrokersAvailableException e) {
      LOG.warn("There is no available brokers");
      throw new GungnirClientException("There is no available brokers");
    } catch (Exception e) {
      LOG.error("Failed to create connection", e);
      throw new GungnirClientException("Failed to create connection", e);
    }
  }

  public static Connection getConnection(String userName, String password)
      throws GungnirServerException, GungnirClientException {
    GungnirConfig config = GungnirConfig.readGugnirConfig();
    return getConnection(config, userName, password);
  }
}
