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

package org.gennai.gungnir.metastore;

import java.util.List;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.ql.FileStat;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.tuple.schema.Schema;

public interface MetaStore {

  void open() throws MetaStoreException;

  void init() throws MetaStoreException;

  void drop() throws MetaStoreException;

  void close();

  void insertUserAccount(UserEntity user) throws MetaStoreException, AlreadyStoredException;

  UserEntity findUserAccountById(String accountId) throws MetaStoreException, NotStoredException;

  UserEntity findUserAccountByName(String userName) throws MetaStoreException, NotStoredException;

  List<UserEntity> findUserAccounts() throws MetaStoreException;

  void updateUserAccount(UserEntity user) throws MetaStoreException, NotStoredException;

  void changeUserAccountPassword(UserEntity user) throws MetaStoreException, NotStoredException;

  void deleteUserAccount(UserEntity user) throws MetaStoreException, NotStoredException;

  void insertSchema(Schema schema) throws MetaStoreException, AlreadyStoredException;

  Schema findSchema(String schemaName, UserEntity owner)
      throws MetaStoreException, NotStoredException;

  boolean changeSchemaToBusy(Schema schema, String topologyId) throws MetaStoreException;

  boolean changeSchemaToFree(Schema schema, String topologyId) throws MetaStoreException;

  List<Schema> findSchemas(UserEntity owner) throws MetaStoreException;

  boolean deleteSchema(Schema schema) throws MetaStoreException, NotStoredException;

  void insertFile(FileStat fileStat) throws MetaStoreException, AlreadyStoredException;

  void changeFileStat(FileStat fileStat) throws MetaStoreException, NotStoredException;

  FileStat findFile(String fileName, UserEntity owner) throws MetaStoreException,
      NotStoredException;

  boolean changeFileToBusy(FileStat fileStat, String topologyId) throws MetaStoreException;

  boolean changeFileToFree(FileStat fileStat, String topologyId) throws MetaStoreException;

  List<FileStat> findFiles(UserEntity owner) throws MetaStoreException;

  boolean deleteFile(FileStat fileStat) throws MetaStoreException, NotStoredException;

  void insertChunk(FileStat fileStat, byte[] chunk) throws MetaStoreException;

  public interface ChunkIterator {

    boolean hasNext();

    byte[] next();

    void close();
  }

  ChunkIterator findChunks(FileStat fileStat) throws MetaStoreException;

  boolean exportFile(FileStat fileStat, String exportDir, int retryTimes, int retryInterval)
      throws MetaStoreException, NotStoredException;

  void deleteChunks(FileStat fileStat) throws MetaStoreException, NotStoredException;

  void insertFunction(FunctionEntity function) throws MetaStoreException, AlreadyStoredException;

  FunctionEntity findFunction(String functionName, UserEntity owner) throws MetaStoreException,
      NotStoredException;

  boolean changeFunctionToBusy(FunctionEntity function, String topologyId)
      throws MetaStoreException;

  boolean changeFunctionToFree(FunctionEntity function, String topologyId)
      throws MetaStoreException;

  List<FunctionEntity> findFunctions(UserEntity owner) throws MetaStoreException;

  boolean deleteFunction(FunctionEntity function) throws MetaStoreException, NotStoredException;

  void insertTopology(GungnirTopology topology) throws MetaStoreException, AlreadyStoredException;

  boolean changeTopologyStatus(GungnirTopology topology) throws MetaStoreException;

  void changeForcedTopologyStatus(GungnirTopology topology) throws MetaStoreException;

  List<GungnirTopology> findTopologies(UserEntity owner) throws MetaStoreException;

  List<GungnirTopology> findTopologies(UserEntity owner, TopologyStatus status)
      throws MetaStoreException;

  GungnirTopology findTopologyById(String topologyId) throws MetaStoreException, NotStoredException;

  GungnirTopology findTopologyByName(String topologyName, UserEntity owner)
      throws MetaStoreException, NotStoredException;

  TopologyStatus getTopologyStatus(String topologyId) throws MetaStoreException, NotStoredException;

  void deleteTopology(GungnirTopology topology) throws MetaStoreException, NotStoredException;

  String generateTrackingId() throws MetaStoreException;

  Integer getTrackingNo(String tid) throws MetaStoreException, NotStoredException;
}
