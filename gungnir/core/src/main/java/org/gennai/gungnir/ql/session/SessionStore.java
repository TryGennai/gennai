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

import org.gennai.gungnir.UserEntity;

public interface SessionStore {

  void open() throws SessionStoreException;

  String createSession(UserEntity owner) throws SessionStoreException;

  String createStatement(String sessionId) throws SessionStoreException, InvalidSessionException;

  StatementEntity getStatement(String statementId) throws SessionStoreException,
      InvalidSessionException;

  void setStatement(String statementId, StatementEntity statement) throws SessionStoreException,
      InvalidSessionException;

  void deleteStatement(String statementId) throws SessionStoreException;

  void deleteSession(String sessionId) throws SessionStoreException;

  void deleteAllSessions(String accountId) throws SessionStoreException;

  void close() throws SessionStoreException;
}
