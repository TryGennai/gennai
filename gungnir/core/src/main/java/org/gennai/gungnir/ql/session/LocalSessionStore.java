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

package org.gennai.gungnir.ql.session;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LocalSessionStore implements SessionStore {

  private static final Logger LOG = LoggerFactory.getLogger(LocalSessionStore.class);

  private int sessionTimeoutSecs;
  private String cacheDir;
  private Map<String, SessionEntity> sessionsMap = Maps.newHashMap();
  private Map<String, StatementEntity> statementsMap = Maps.newHashMap();
  private Map<String, Map<String, Set<String>>> sessionIndex = Maps.newHashMap();

  public LocalSessionStore() {
    GungnirConfig config = GungnirManager.getManager().getConfig();
    sessionTimeoutSecs = config.getInteger(SESSION_TIMEOUT_SECS);
    cacheDir = config.getString(LOCAL_DIR) + "/" + SESSION_CACHE_DIR;
  }

  @Override
  public void open() throws SessionStoreException {
    Path path = Paths.get(cacheDir);
    if (Files.exists(path)) {
      try {
        GungnirUtils.deleteDirectory(path);
      } catch (IOException ignore) {
        ignore = null;
      }
    }
  }

  @Override
  public synchronized String createSession(UserEntity owner) throws SessionStoreException {
    Map<String, Set<String>> sessions = sessionIndex.get(owner.getId());
    if (sessions == null) {
      sessions = Maps.newHashMap();
      sessionIndex.put(owner.getId(), sessions);
    } else {
      for (String sessionId : sessions.keySet()) {
        SessionEntity session = sessionsMap.get(sessionId);
        if (session != null) {
          if (session.isExpired()) {
            deleteSession(sessionId);
            LOG.info("Expired session {}", sessionId);
          } else {
            LOG.info("Update session timeout. session: {}, timeout: {}", sessionId,
                session.getExpire());
          }
        }
      }
    }

    SessionEntity session = new SessionEntity(UUID.randomUUID().toString(), owner.getId(),
        sessionTimeoutSecs);
    sessionsMap.put(session.getSessionId(), session);

    sessions.put(session.getSessionId(), Sets.<String>newHashSet());

    LOG.info("Successful to create session {}", session.getSessionId());
    return session.getSessionId();
  }

  @Override
  public synchronized String createStatement(String sessionId) throws SessionStoreException,
      InvalidSessionException {
    SessionEntity session = sessionsMap.get(sessionId);
    if (session == null) {
      throw new InvalidSessionException("This session has been closed");
    }

    if (session.isExpired()) {
      deleteSession(sessionId);
      LOG.info("Expired session {}", sessionId);
      throw new InvalidSessionException("This session has been closed");
    } else {
      LOG.info("Update session timeout. session: {}, timeout: {}", sessionId, session.getExpire());
    }

    UserEntity owner = null;
    try {
      owner =
          GungnirManager.getManager().getMetaStore().findUserAccountById(session.getAccountId());
    } catch (MetaStoreException e) {
      LOG.error("Failed to create statement", e);
      throw new SessionStoreException(e);
    } catch (NotStoredException e) {
      throw new InvalidSessionException("This session has been closed");
    }

    StatementEntity statement = new StatementEntity(UUID.randomUUID().toString(), sessionId, owner);
    statementsMap.put(statement.getStatementId(), statement);

    Map<String, Set<String>> sessions = sessionIndex.get(owner.getId());
    if (sessions == null) {
      sessions = Maps.newHashMap();
      sessionIndex.put(owner.getId(), sessions);

      Set<String> statements = Sets.newHashSet();
      sessions.put(sessionId, statements);

      statements.add(statement.getStatementId());
    } else {
      Set<String> statements = sessions.get(sessionId);
      if (statements == null) {
        statements = Sets.newHashSet();
        sessions.put(sessionId, statements);
      }

      statements.add(statement.getStatementId());
    }

    LOG.info("Successful to create statement {}", statement.getStatementId());
    return statement.getStatementId();
  }

  @Override
  public synchronized StatementEntity getStatement(String statementId)
      throws SessionStoreException, InvalidSessionException {
    StatementEntity statement = statementsMap.get(statementId);
    if (statement == null) {
      throw new InvalidSessionException("This session has been closed");
    }

    if (statement.getTopology() != null) {
      if (statement.getTopology().getId() != null) {
        try {
          TopologyStatus status = GungnirManager.getManager().getMetaStore()
              .getTopologyStatus(statement.getTopology().getId());
          statement.getTopology().setStatus(status);
        } catch (NotStoredException e) {
          statement.clear();
        } catch (MetaStoreException e) {
          LOG.error("Failed to get statement", e);
          throw new SessionStoreException(e);
        }
      }
    }

    SessionEntity session = sessionsMap.get(statement.getSessionId());
    if (session == null) {
      throw new InvalidSessionException("This session has been closed");
    }

    if (session.isExpired()) {
      deleteSession(statement.getSessionId());
      throw new InvalidSessionException("This session has been closed");
    } else {
      LOG.info("Update session timeout. session: {}, timeout: {}", statement.getSessionId(),
          session.getExpire());
    }

    return statement;
  }

  @Override
  public synchronized void setStatement(String statementId, StatementEntity statement)
      throws SessionStoreException, InvalidSessionException {
    SessionEntity session = sessionsMap.get(statement.getSessionId());
    if (session == null) {
      throw new InvalidSessionException("This session has been closed");
    }

    if (session.isExpired()) {
      deleteSession(statement.getSessionId());
      throw new InvalidSessionException("This session has been closed");
    } else {
      LOG.info("Update session timeout. session: {}, timeout: {}", statement.getSessionId(),
          session.getExpire());
    }

    LOG.info("Successful to set statement {}", statementId);
  }

  @Override
  public synchronized void deleteStatement(String statementId) throws SessionStoreException {
    StatementEntity statement = statementsMap.remove(statementId);

    if (statement != null) {
      Map<String, Set<String>> sessions = sessionIndex.get(statement.getOwner().getId());
      if (sessions != null) {
        Set<String> statements = sessions.get(statement.getSessionId());
        if (statements != null) {
          statements.remove(statementId);
        }
      }

      try {
        GungnirUtils.deleteDirectory(Paths.get(cacheDir, statement.getSessionId(),
            statement.getStatementId()));
      } catch (IOException e) {
        LOG.error("Failed to delete statement", e);
        throw new SessionStoreException(e);
      }
    }

    LOG.info("Successful to delete statement {}", statementId);
  }

  @Override
  public synchronized void deleteSession(String sessionId) throws SessionStoreException {
    SessionEntity session = sessionsMap.remove(sessionId);
    if (session != null) {
      Map<String, Set<String>> sessions = sessionIndex.get(session.getAccountId());
      if (sessions != null) {
        Set<String> statements = sessions.remove(sessionId);
        if (statements != null) {
          for (String statementId : statements) {
            statementsMap.remove(statementId);
          }
        }

        if (sessions.isEmpty()) {
          sessionIndex.remove(session.getAccountId());
        }
      }

      try {
        GungnirUtils.deleteDirectory(Paths.get(cacheDir, session.getSessionId()));
      } catch (IOException e) {
        LOG.error("Failed to delete session", e);
        throw new SessionStoreException(e);
      }
    }

    LOG.info("Successful to delete session {}", sessionId);
  }

  @Override
  public synchronized void deleteAllSessions(String accountId) throws SessionStoreException {
    Map<String, Set<String>> sessions = sessionIndex.remove(accountId);
    if (sessions != null) {
      for (Map.Entry<String, Set<String>> entry : sessions.entrySet()) {
        sessionsMap.remove(entry.getKey());
        LOG.info("Successful to delete session {}", entry.getKey());

        for (String statementId : entry.getValue()) {
          statementsMap.remove(statementId);
          LOG.info("Successful to delete statement {}", statementId);
        }

        try {
          GungnirUtils.deleteDirectory(Paths.get(cacheDir, entry.getKey()));
        } catch (IOException e) {
          LOG.error("Failed to delete session", e);
          throw new SessionStoreException(e);
        }
      }
    }
  }

  @Override
  public void close() throws SessionStoreException {
  }
}
