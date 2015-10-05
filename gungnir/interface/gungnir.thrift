/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.gennai.gungnir.thrift

enum ErrorCode {
  ERROR_NONE = 0,
  ERROR_ACCESS_DENIED = 1,
  ERROR_INVALID_SESSIONID = 2,
  ERROR_INVALID_STATEMENTID = 3,
  ERROR_EXECUTE_COMMAND_FAILED = 4,
  ERROR_UPLOAD_FILE_FAILED = 5,
  ERROR_INTERNAL_ERROR = 99
}

exception GungnirServerException {
  1: ErrorCode code,
  2: string message
}

service GungnirService {
  string createConnection(1: string userName, 2: string password) throws (1: GungnirServerException e),
  string createStatement(1: string sessionId) throws (1: GungnirServerException e),
  string execute(1: string statementId, 2: string command) throws (1: GungnirServerException e),
  void beginFileUpload(1: string statementId, 2: string fileName) throws (1: GungnirServerException e),
  void uploadChunk(1: string statementId, 2: binary chunk) throws (1: GungnirServerException e),
  void finishFileUpload(1: string statementId, 2: i32 fileSize, 3: i64 checksum) throws (1: GungnirServerException e),
  void closeStatement(1: string statementId),
  void closeConnection(1: string sessionId)
}

