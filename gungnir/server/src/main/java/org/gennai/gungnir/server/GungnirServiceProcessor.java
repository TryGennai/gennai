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

package org.gennai.gungnir.server;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.CommandProcessor;
import org.gennai.gungnir.ql.CommandProcessorException;
import org.gennai.gungnir.ql.CommandProcessorFactory;
import org.gennai.gungnir.ql.UploadProcessor;
import org.gennai.gungnir.ql.UploadProcessorException;
import org.gennai.gungnir.ql.session.InvalidSessionException;
import org.gennai.gungnir.ql.session.SessionStore;
import org.gennai.gungnir.ql.session.SessionStoreException;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.thrift.ErrorCode;
import org.gennai.gungnir.thrift.GungnirServerException;
import org.gennai.gungnir.thrift.GungnirService;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.util.Future;
import com.twitter.util.Promise;

public class GungnirServiceProcessor implements GungnirService.ServiceIface {

  private static final Logger LOG = LoggerFactory.getLogger(GungnirServiceProcessor.class);

  private Executor executor;
  private SessionStore sessionStore;
  private CommandProcessorFactory processorFactory;

  public GungnirServiceProcessor() {
    executor = Executors.newCachedThreadPool(
        GungnirUtils.createThreadFactory("GungnirServiceProcessor"));

    try {
      sessionStore = GungnirManager.getManager().getClusterManager().getSessionStore();
    } catch (SessionStoreException e) {
      throw new RuntimeException(e);
    }

    processorFactory = new CommandProcessorFactory();
  }

  @Override
  public Future<String> createConnection(final String userName, final String password) {
    final Promise<String> promise = new Promise<String>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          UserEntity user = GungnirManager.getManager().getMetaStore()
              .findUserAccountByName(userName);
          if (user.validatePassword(password)) {
            String sessionId = sessionStore.createSession(user);
            promise.setValue(sessionId);
            LOG.info("Established the session with '{}' session: '{}'", userName, sessionId);
          } else {
            promise.setException(new GungnirServerException(ErrorCode.ERROR_ACCESS_DENIED,
                "Access denied for user '" + userName + "'"));
          }
        } catch (MetaStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        } catch (NotStoredException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_ACCESS_DENIED,
              "Access denied for user '" + userName + "'"));
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        }
      }
    });

    return promise;
  }

  @Override
  public Future<String> createStatement(final String sessionId) {
    final Promise<String> promise = new Promise<String>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          promise.setValue(sessionStore.createStatement(sessionId));
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        } catch (InvalidSessionException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INVALID_SESSIONID,
              "This session has been disconnected"));
        }
      }
    });

    return promise;
  }

  @Override
  public Future<String> execute(final String statementId, final String command) {
    final Promise<String> promise = new Promise<String>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        LOG.info("Execute command '{}' statement: '{}'", command, statementId);

        try {
          StatementEntity statement = sessionStore.getStatement(statementId);

          String cmd = command.trim();
          CommandProcessor processor = processorFactory.getProcessor(statement, cmd);
          if (processor != null) {
            LOG.debug("Compile '{}'", cmd);
            try {
              String res = processor.run(statement, cmd);
              sessionStore.setStatement(statementId, statement);
              promise.setValue(res);
            } catch (CommandProcessorException e) {
              LOG.error("Failed to execute command '{}'", cmd, e);
              promise.setException(new GungnirServerException(
                  ErrorCode.ERROR_EXECUTE_COMMAND_FAILED, e.getCause().getMessage()));
            }
          } else {
            promise.setException(new GungnirServerException(ErrorCode.ERROR_EXECUTE_COMMAND_FAILED,
                "Command isn't supported"));
          }
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        } catch (InvalidSessionException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INVALID_SESSIONID,
              "This session has been disconnected"));
        }
      }
    });

    return promise;
  }

  @Override
  public Future<Void> beginFileUpload(final String statementId, final String fileName) {
    final Promise<Void> promise = new Promise<Void>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          StatementEntity statement = sessionStore.getStatement(statementId);

          try {
            UploadProcessor.beginUpload(statement, fileName);
            sessionStore.setStatement(statementId, statement);
            promise.setValue(null);
          } catch (UploadProcessorException e) {
            LOG.error("Failed to upload file '{}'", fileName, e);
            promise.setException(new GungnirServerException(
                ErrorCode.ERROR_UPLOAD_FILE_FAILED, e.getCause().getMessage()));
          }
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        } catch (InvalidSessionException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INVALID_SESSIONID,
              "This session has been disconnected"));
        }
      }
    });

    return promise;
  }

  @Override
  public Future<Void> uploadChunk(final String statementId, final ByteBuffer chunk) {
    final Promise<Void> promise = new Promise<Void>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          StatementEntity statement = sessionStore.getStatement(statementId);

          try {
            UploadProcessor.writeChunk(statement, chunk);
            sessionStore.setStatement(statementId, statement);
            promise.setValue(null);
          } catch (UploadProcessorException e) {
            LOG.error("Failed to upload file '{}'", statement.getUploadingFileName(), e);
            promise.setException(new GungnirServerException(
                ErrorCode.ERROR_UPLOAD_FILE_FAILED, e.getCause().getMessage()));
          }
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        } catch (InvalidSessionException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INVALID_SESSIONID,
              "This session has been disconnected"));
        }
      }
    });

    return promise;
  }


  @Override
  public Future<Void> finishFileUpload(final String statementId, final int fileSize,
      final long checksum) {
    final Promise<Void> promise = new Promise<Void>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          StatementEntity statement = sessionStore.getStatement(statementId);

          try {
            UploadProcessor.finishUpload(statement, fileSize, checksum);
            sessionStore.setStatement(statementId, statement);
            promise.setValue(null);
          } catch (UploadProcessorException e) {
            LOG.error("Failed to upload file '{}'", statement.getUploadingFileName(), e);
            promise.setException(new GungnirServerException(
                ErrorCode.ERROR_UPLOAD_FILE_FAILED, e.getCause().getMessage()));
          }
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        } catch (InvalidSessionException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INVALID_SESSIONID,
              "This session has been disconnected"));
        }
      }
    });

    return promise;
  }

  @Override
  public Future<Void> closeStatement(final String statementId) {
    final Promise<Void> promise = new Promise<Void>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          sessionStore.deleteStatement(statementId);
          promise.setValue(null);
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        }
      }
    });

    return promise;
  }

  @Override
  public Future<Void> closeConnection(final String sessionId) {
    final Promise<Void> promise = new Promise<Void>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          sessionStore.deleteSession(sessionId);
          promise.setValue(null);
        } catch (SessionStoreException e) {
          promise.setException(new GungnirServerException(ErrorCode.ERROR_INTERNAL_ERROR,
              e.getMessage()));
        }
      }
    });

    return promise;
  }
}
