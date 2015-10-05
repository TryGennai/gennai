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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.ql.FileStat;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class InMemoryMetaStore implements MetaStore {

  private static Logger LOG = LoggerFactory.getLogger(InMemoryMetaStore.class);

  private String storePath;
  private Map<String, UserEntity> usersById = Maps.newHashMap();
  private Map<String, UserEntity> usersByName = Maps.newHashMap();
  private Map<String, Map<String, Schema>> schemasByName = Maps.newHashMap();
  private Map<String, Map<String, FileStat>> filesByName = Maps.newHashMap();
  private Map<String, Map<String, FunctionEntity>> functionsByName = Maps.newHashMap();
  private Map<String, GungnirTopology> topologiesById = Maps.newHashMap();
  private Map<String, Map<String, GungnirTopology>> topologiesByName = Maps.newHashMap();
  private Map<String, Integer> trackingIds = Maps.newHashMap();
  private int currentTrackingId;

  @Override
  public synchronized void open() throws MetaStoreException {
    GungnirConfig config = GungnirManager.getManager().getConfig();
    storePath = config.getString(LOCAL_DIR) + "/" + STORE_DIR;
  }

  @Override
  public synchronized void init() throws MetaStoreException {
    UserEntity rootUser = null;
    while (rootUser == null) {
      try {
        rootUser = findUserAccountByName(ROOT_USER_NAME);
      } catch (NotStoredException e) {
        try {
          rootUser = new UserEntity(ROOT_USER_NAME);
          rootUser.setPassword(ROOT_USER_PASSWORD);
          insertUserAccount(rootUser);
        } catch (AlreadyStoredException ignore) {
          ignore = null;
        }
      }
    }

    Path path = Paths.get(storePath);
    try {
      GungnirUtils.deleteDirectory(path);
      Files.createDirectories(path);
    } catch (IOException e) {
      LOG.error("Failed to initialize metastore", e);
    }
  }

  @Override
  public synchronized void drop() throws MetaStoreException {
    usersById = Maps.newHashMap();
    usersByName = Maps.newHashMap();
    schemasByName = Maps.newHashMap();
    filesByName = Maps.newHashMap();
    functionsByName = Maps.newHashMap();
    topologiesById = Maps.newHashMap();
    topologiesByName = Maps.newHashMap();
    trackingIds = Maps.newHashMap();
    currentTrackingId = 0;

    Path path = Paths.get(storePath);
    try {
      if (Files.exists(path)) {
        GungnirUtils.deleteDirectory(path);
      }
    } catch (IOException e) {
      LOG.error("Failed to drop metastore", e);
      throw new MetaStoreException("Failed to drop metastore", e);
    }
  }

  @Override
  public synchronized void close() {
  }

  @Override
  public synchronized void insertUserAccount(UserEntity user) throws MetaStoreException,
      AlreadyStoredException {
    if (!usersByName.containsKey(user.getName())) {
      Date createTime = new Date();
      user.setId(generateUniqueId());
      user.setCreateTime(createTime);
      user.setLastModifyTime(createTime);
      UserEntity userCopy = user.clone();
      usersById.put(user.getId(), userCopy);
      usersByName.put(user.getName(), userCopy);

      LOG.info("Successful to insert user account '{}'", user.getName());
    } else {
      throw new AlreadyStoredException("'" + user.getName() + "' already exists");
    }
  }

  @Override
  public synchronized UserEntity findUserAccountById(String accountId)
      throws MetaStoreException, NotStoredException {
    UserEntity storedUser = usersById.get(accountId);
    if (storedUser != null) {
      return storedUser.clone();
    } else {
      throw new NotStoredException("Can't find user account '" + accountId + "'");
    }
  }

  @Override
  public synchronized UserEntity findUserAccountByName(String userName)
      throws MetaStoreException, NotStoredException {
    UserEntity storedUser = usersByName.get(userName);
    if (storedUser != null) {
      return storedUser.clone();
    } else {
      throw new NotStoredException("'" + userName + "' isn't registered");
    }
  }

  @Override
  public synchronized List<UserEntity> findUserAccounts() throws MetaStoreException {
    List<UserEntity> results = Lists.newArrayList();
    for (UserEntity user : usersById.values()) {
      results.add(user.clone());
    }
    return results;
  }

  @Override
  public synchronized void updateUserAccount(UserEntity user) throws MetaStoreException,
      NotStoredException {
    UserEntity storedUser = usersById.get(user.getId());
    if (storedUser != null) {
      usersByName.remove(storedUser.getName());

      user.setLastModifyTime(new Date());
      storedUser.setName(user.getName());
      storedUser.setPassword(user.getPassword());
      storedUser.setLastModifyTime(user.getLastModifyTime());

      usersByName.put(storedUser.getName(), storedUser);

      LOG.info("Successful to update user account '{}'", user.getName());
    } else {
      throw new NotStoredException("Can't find user account '" + user.getId() + "'");
    }
  }

  @Override
  public synchronized void changeUserAccountPassword(UserEntity user) throws MetaStoreException,
      NotStoredException {
    UserEntity storedUser = usersById.get(user.getId());
    if (storedUser != null) {
      user.setLastModifyTime(new Date());
      storedUser.setPassword(user.getPassword());
      storedUser.setLastModifyTime(user.getLastModifyTime());

      LOG.info("Successful to change password '{}'", user.getName());
    } else {
      throw new NotStoredException("Can't find user account '" + user.getId() + "'");
    }
  }

  @Override
  public synchronized void deleteUserAccount(UserEntity user) throws MetaStoreException,
      NotStoredException {
    if (usersById.remove(user.getId()) != null) {
      usersByName.remove(user.getName());

      LOG.info("Successful to delete user account '{}'", user.getName());
    } else {
      throw new NotStoredException("Can't find user account '" + user.getId() + "'");
    }
  }

  @Override
  public synchronized void insertSchema(Schema schema) throws MetaStoreException,
      AlreadyStoredException {
    Map<String, Schema> schemas = schemasByName.get(schema.getOwner().getId());
    if (schemas != null) {
      if (schemas.containsKey(schema.getSchemaName())) {
        throw new AlreadyStoredException(schema.getSchemaName() + " already exists");
      }
    } else {
      schemas = Maps.newHashMap();
      schemasByName.put(schema.getOwner().getId(), schemas);
    }

    schema.setId(generateUniqueId());
    schema.setCreateTime(new Date());

    schemas.put(schema.getSchemaName(), schema.clone());

    LOG.info("Successful to insert schema {} owned by {}", schema.getSchemaName(),
        schema.getOwner().getName());
  }

  @Override
  public synchronized Schema findSchema(String schemaName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    Map<String, Schema> schemas = schemasByName.get(owner.getId());
    Schema storedSchema = null;
    if (schemas != null) {
      storedSchema = schemas.get(schemaName);
    }

    if (storedSchema != null) {
      return storedSchema.clone();
    } else {
      throw new NotStoredException(schemaName + " isn't registered");
    }
  }

  @Override
  public synchronized boolean changeSchemaToBusy(Schema schema, String topologyId)
      throws MetaStoreException {
    Map<String, Schema> schemas = schemasByName.get(schema.getOwner().getId());
    Schema storedSchema = null;
    if (schemas != null) {
      storedSchema = schemas.get(schema.getSchemaName());
    }

    if (storedSchema != null && schema.getCreateTime().equals(storedSchema.getCreateTime())
        && !storedSchema.getTopologies().contains(topologyId)) {
      storedSchema.getTopologies().add(topologyId);

      LOG.info("Successful to change status to busy {} owned by {}", schema.getSchemaName(),
          schema.getOwner().getName());
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean changeSchemaToFree(Schema schema, String topologyId)
      throws MetaStoreException {
    Map<String, Schema> schemas = schemasByName.get(schema.getOwner().getId());
    Schema storedSchema = null;
    if (schemas != null) {
      storedSchema = schemas.get(schema.getSchemaName());
    }

    if (storedSchema != null && schema.getCreateTime().equals(storedSchema.getCreateTime())
        && storedSchema.getTopologies().contains(topologyId)) {
      storedSchema.getTopologies().remove(topologyId);

      LOG.info("Successful to change status to free {} owned by {}", schema.getSchemaName(),
          schema.getOwner().getName());
      return true;
    }
    return false;
  }

  @Override
  public synchronized List<Schema> findSchemas(UserEntity owner) throws MetaStoreException {
    List<Schema> results = Lists.newArrayList();
    Map<String, Schema> schemas = schemasByName.get(owner.getId());
    if (schemas != null) {
      for (Schema schema : schemas.values()) {
        results.add(schema.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized boolean deleteSchema(Schema schema) throws MetaStoreException,
      NotStoredException {
    Map<String, Schema> schemas = schemasByName.get(schema.getOwner().getId());
    Schema storedSchema = null;
    if (schemas != null) {
      storedSchema = schemas.get(schema.getSchemaName());
    }

    if (storedSchema != null) {
      if (storedSchema.getTopologies().isEmpty()) {
        schemas.remove(schema.getSchemaName());
        return true;
      } else {
        return false;
      }
    } else {
      throw new NotStoredException("Can't find schema '" + schema.getId() + "'");
    }
  }

  @Override
  public synchronized void insertFile(FileStat fileStat) throws MetaStoreException,
      AlreadyStoredException {
    Map<String, FileStat> files = filesByName.get(fileStat.getOwner().getId());
    if (files != null) {
      if (files.containsKey(fileStat.getName())) {
        throw new AlreadyStoredException("'" + fileStat.getName() + "' already exists");
      }
    } else {
      files = Maps.newHashMap();
      filesByName.put(fileStat.getOwner().getId(), files);
    }

    fileStat.setId(generateUniqueId());
    fileStat.setCreateTime(new Date());

    files.put(fileStat.getName(), fileStat.clone());

    LOG.info("Successful to insert file '{}' owned by {}", fileStat.getName(),
        fileStat.getOwner().getName());
  }

  @Override
  public synchronized void changeFileStat(FileStat fileStat) throws MetaStoreException,
      NotStoredException {
    Map<String, FileStat> files = filesByName.get(fileStat.getOwner().getId());
    FileStat storedFileStat = null;
    if (files != null) {
      storedFileStat = files.get(fileStat.getName());
    }

    if (storedFileStat != null) {
      storedFileStat.setSize(fileStat.getSize());
      storedFileStat.setChecksum(fileStat.getChecksum());
      storedFileStat.setCreateTime(new Date());

      LOG.info("Successful to update stat '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
    } else {
      throw new NotStoredException("Can't find file '" + fileStat.getName() + "'");
    }
  }

  @Override
  public synchronized FileStat findFile(String fileName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    Map<String, FileStat> files = filesByName.get(owner.getId());
    FileStat storedFileStat = null;
    if (files != null) {
      storedFileStat = files.get(fileName);
    }

    if (storedFileStat != null) {
      return storedFileStat.clone();
    } else {
      throw new NotStoredException("'" + fileName + "' isn't registered");
    }
  }

  @Override
  public synchronized boolean changeFileToBusy(FileStat fileStat, String topologyId)
      throws MetaStoreException {
    Map<String, FileStat> files = filesByName.get(fileStat.getOwner().getId());
    FileStat storedFile = null;
    if (files != null) {
      storedFile = files.get(fileStat.getName());
    }

    if (storedFile != null && fileStat.getCreateTime().equals(storedFile.getCreateTime())
        && !storedFile.getTopologies().contains(topologyId)) {
      storedFile.getTopologies().add(topologyId);

      LOG.info("Successful to change stat to busy '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean changeFileToFree(FileStat fileStat, String topologyId)
      throws MetaStoreException {
    Map<String, FileStat> files = filesByName.get(fileStat.getOwner().getId());
    FileStat storedFile = null;
    if (files != null) {
      storedFile = files.get(fileStat.getName());
    }

    if (storedFile != null && fileStat.getCreateTime().equals(storedFile.getCreateTime())
        && storedFile.getTopologies().contains(topologyId)) {
      storedFile.getTopologies().remove(topologyId);

      LOG.info("Successful to change stat to free '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
      return true;
    }
    return false;
  }

  @Override
  public synchronized List<FileStat> findFiles(UserEntity owner) throws MetaStoreException {
    List<FileStat> results = Lists.newArrayList();
    Map<String, FileStat> files = filesByName.get(owner.getId());
    if (files != null) {
      for (FileStat file : files.values()) {
        results.add(file.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized boolean deleteFile(FileStat fileStat) throws MetaStoreException,
      NotStoredException {
    Map<String, FileStat> files = filesByName.get(fileStat.getOwner().getId());
    FileStat storedFile = null;
    if (files != null) {
      storedFile = files.get(fileStat.getName());
    }

    if (storedFile != null) {
      if (storedFile.getSize() > 0 && storedFile.getTopologies().isEmpty()) {
        files.remove(fileStat.getName());
        return true;
      } else {
        return false;
      }
    } else {
      throw new NotStoredException("Can't find file '" + fileStat.getId() + "'");
    }
  }

  @Override
  public void insertChunk(FileStat fileStat, byte[] chunk) throws MetaStoreException {
    SeekableByteChannel sbc = null;
    try {
      sbc = Files.newByteChannel(Paths.get(storePath, fileStat.getId()), StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);

      sbc.write(ByteBuffer.wrap(chunk));

      LOG.info("Successful to insert chunk '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
    } catch (IOException e) {
      LOG.error("Failed to insert chunk", e);
      throw new MetaStoreException("Failed to insert chunk", e);
    } finally {
      if (sbc != null) {
        try {
          sbc.close();
        } catch (IOException e) {
          LOG.error("Failed to close file", e);
          throw new MetaStoreException("Failed to close file", e);
        }
      }
    }
  }

  public static class FileChunkIterator implements ChunkIterator {

    private SeekableByteChannel sbc;
    private ByteBuffer buff = ByteBuffer.allocate(8192);
    private int size = -1;

    public FileChunkIterator(SeekableByteChannel sbc) {
      this.sbc = sbc;
    }

    @Override
    public boolean hasNext() {
      try {
        size = sbc.read(buff);
        return size != -1;
      } catch (IOException e) {
        LOG.error("Failed to read chunk", e);
      }
      return false;
    }

    @Override
    public byte[] next() {
      if (size >= 0) {
        buff.flip();
        byte[] bytes = new byte[buff.limit()];
        buff.get(bytes);
        buff.clear();
        return bytes;
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void close() {
      try {
        sbc.close();
      } catch (IOException e) {
        LOG.error("Failed to close file", e);
      }
    }
  }

  @Override
  public ChunkIterator findChunks(FileStat fileStat) throws MetaStoreException {
    SeekableByteChannel sbc = null;
    try {
      sbc = Files.newByteChannel(Paths.get(storePath, fileStat.getId()), StandardOpenOption.READ);

      LOG.info("Successful to insert chunk '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
      return new FileChunkIterator(sbc);
    } catch (IOException e) {
      LOG.error("Failed to find chunks", e);
      throw new MetaStoreException("Failed to find chunks", e);
    }
  }

  @Override
  public synchronized boolean exportFile(FileStat fileStat, String exportDir, int retryTimes,
      int retryInterval) throws MetaStoreException, NotStoredException {
    for (int i = 0; i < retryTimes; i++) {
      for (int j = 0; j < retryTimes; j++) {
        if (fileStat.getSize() == 0) {
          try {
            TimeUnit.MILLISECONDS.sleep(retryInterval);
          } catch (InterruptedException e) {
            throw new MetaStoreException("Failed to export file", e);
          }
          fileStat = findFile(fileStat.getName(), fileStat.getOwner());
        } else {
          break;
        }
      }
      if (fileStat.getSize() == 0) {
        return false;
      }

      try {
        Path path = Paths.get(exportDir);
        if (!Files.exists(path)) {
          Files.createDirectories(path);
        }
        Files.createLink(Paths.get(exportDir, fileStat.getName()),
            Paths.get(storePath, fileStat.getId()));
      } catch (IOException e) {
        throw new MetaStoreException("Failed to export file", e);
      }

      FileStat f = findFile(fileStat.getName(), fileStat.getOwner());
      if (f.getCreateTime() == fileStat.getCreateTime()) {
        return true;
      }
      fileStat = f;
    }

    return false;
  }

  @Override
  public synchronized void deleteChunks(FileStat fileStat) throws MetaStoreException,
      NotStoredException {
    try {
      Files.delete(Paths.get(storePath, fileStat.getId()));
      LOG.info("Successful to delete chunks '{}'", fileStat.getId());
    } catch (NoSuchFileException e) {
      throw new NotStoredException("Can't find chunks '" + fileStat.getId() + "'");
    } catch (IOException e) {
      LOG.error("Failed to delete chunks", e);
      throw new MetaStoreException("Failed to delete chunks", e);
    }
  }

  @Override
  public synchronized void insertFunction(FunctionEntity function) throws MetaStoreException,
      AlreadyStoredException {
    Map<String, FunctionEntity> functions = functionsByName.get(function.getOwner().getId());
    if (functions != null) {
      if (functions.containsKey(function.getName())) {
        throw new AlreadyStoredException(function.getName() + " already exists");
      }
    } else {
      functions = Maps.newHashMap();
      functionsByName.put(function.getOwner().getId(), functions);
    }

    function.setId(generateUniqueId());
    function.setCreateTime(new Date());

    functions.put(function.getName(), function.clone());

    LOG.info("Successful to insert function {} owned by {}", function.getName(),
        function.getOwner().getName());
  }

  @Override
  public synchronized FunctionEntity findFunction(String functionName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    Map<String, FunctionEntity> functions = functionsByName.get(owner.getId());
    FunctionEntity storedFunction = null;
    if (functions != null) {
      storedFunction = functions.get(functionName);
    }

    if (storedFunction != null) {
      return storedFunction.clone();
    } else {
      throw new NotStoredException(functionName + " isn't registered");
    }
  }

  @Override
  public synchronized boolean changeFunctionToBusy(FunctionEntity function, String topologyId)
      throws MetaStoreException {
    Map<String, FunctionEntity> functions = functionsByName.get(function.getOwner().getId());
    FunctionEntity storedFunction = null;
    if (functions != null) {
      storedFunction = functions.get(function.getName());
    }

    if (storedFunction != null && function.getCreateTime().equals(storedFunction.getCreateTime())
        && !storedFunction.getTopologies().contains(topologyId)) {
      storedFunction.getTopologies().add(topologyId);

      LOG.info("Successful to change status to busy {} owned by {}", function.getName(),
          function.getOwner().getName());
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean changeFunctionToFree(FunctionEntity function, String topologyId)
      throws MetaStoreException {
    Map<String, FunctionEntity> functions = functionsByName.get(function.getOwner().getId());
    FunctionEntity storedFunction = null;
    if (functions != null) {
      storedFunction = functions.get(function.getName());
    }

    if (storedFunction != null && function.getCreateTime().equals(storedFunction.getCreateTime())
        && storedFunction.getTopologies().contains(topologyId)) {
      storedFunction.getTopologies().remove(topologyId);

      LOG.info("Successful to change status to free {} owned by {}", function.getName(),
          function.getOwner().getName());
      return true;
    }
    return false;
  }

  @Override
  public synchronized List<FunctionEntity> findFunctions(UserEntity owner)
      throws MetaStoreException {
    List<FunctionEntity> results = Lists.newArrayList();
    Map<String, FunctionEntity> functions = functionsByName.get(owner.getId());
    if (functions != null) {
      for (FunctionEntity function : functions.values()) {
        results.add(function.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized boolean deleteFunction(FunctionEntity function) throws MetaStoreException,
      NotStoredException {
    Map<String, FunctionEntity> functions = functionsByName.get(function.getOwner().getId());
    FunctionEntity storedFunction = null;
    if (functions != null) {
      storedFunction = functions.get(function.getName());
    }

    if (storedFunction != null) {
      if (storedFunction.getTopologies().isEmpty()) {
        functions.remove(function.getName());
        return true;
      } else {
        return false;
      }
    } else {
      throw new NotStoredException("Can't find function '" + function.getId() + "'");
    }
  }

  @Override
  public synchronized void insertTopology(GungnirTopology topology) throws MetaStoreException,
      AlreadyStoredException {
    Map<String, GungnirTopology> topologies = topologiesByName.get(topology.getOwner().getId());
    if (topologies != null) {
      if (topologies.containsKey(topology.getName())) {
        throw new AlreadyStoredException(topology.getName() + " already exists");
      }
    } else {
      topologies = Maps.newHashMap();
      topologiesByName.put(topology.getOwner().getId(), topologies);
    }

    topology.setId(generateUniqueId());
    topology.setCreateTime(new Date());

    GungnirTopology topologyCopy = topology.clone();

    topologiesById.put(topology.getId(), topologyCopy);
    topologies.put(topology.getName(), topologyCopy);

    LOG.info("Successful to insert topology {}  owned by {}", topology.getName(),
        topology.getOwner().getName());
  }

  @Override
  public synchronized boolean changeTopologyStatus(GungnirTopology topology)
      throws MetaStoreException {
    TopologyStatus status = null;
    switch (topology.getStatus()) {
      case STARTING:
        status = TopologyStatus.STOPPED;
        break;
      case RUNNING:
        status = TopologyStatus.STARTING;
        break;
      case STOPPING:
        status = TopologyStatus.RUNNING;
        break;
      case STOPPED:
        status = TopologyStatus.STOPPING;
        break;
      default:
        return false;
    }

    GungnirTopology storedTopology = topologiesById.get(topology.getId());
    if (storedTopology != null && storedTopology.getStatus() == status) {
      storedTopology.setStatus(topology.getStatus());

      LOG.info("Successful to change topology status '{}' ({}->{})", topology.getId(), status,
          topology.getStatus());
      return true;
    }
    return false;
  }

  @Override
  public synchronized void changeForcedTopologyStatus(GungnirTopology topology)
      throws MetaStoreException {
    GungnirTopology storedTopology = topologiesById.get(topology.getId());
    if (storedTopology != null) {
      storedTopology.setStatus(topology.getStatus());
    }
  }

  @Override
  public synchronized List<GungnirTopology> findTopologies(UserEntity owner)
      throws MetaStoreException {
    List<GungnirTopology> results = Lists.newArrayList();
    Map<String, GungnirTopology> topologies = topologiesByName.get(owner.getId());
    if (topologies != null) {
      for (GungnirTopology topology : topologies.values()) {
        results.add(topology.clone());
      }
    }
    return results;
  }

  @Override
  public synchronized List<GungnirTopology> findTopologies(UserEntity owner, TopologyStatus status)
      throws MetaStoreException {
    List<GungnirTopology> results = Lists.newArrayList();
    Map<String, GungnirTopology> topologies = topologiesByName.get(owner.getId());
    if (topologies != null) {
      for (GungnirTopology topology : topologies.values()) {
        if (topology.getStatus() == status) {
          results.add(topology.clone());
        }
      }
    }
    return results;
  }

  @Override
  public synchronized GungnirTopology findTopologyById(String topologyId)
      throws MetaStoreException, NotStoredException {
    GungnirTopology topology = topologiesById.get(topologyId);
    if (topology != null) {
      return topology.clone();
    } else {
      throw new NotStoredException("Can't find topology '" + topologyId + "'");
    }
  }

  @Override
  public synchronized GungnirTopology findTopologyByName(String topologyName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    Map<String, GungnirTopology> topologies = topologiesByName.get(owner.getId());
    GungnirTopology storedTopology = null;
    if (topologies != null) {
      storedTopology = topologies.get(topologyName);
    }

    if (storedTopology != null) {
      return storedTopology.clone();
    } else {
      throw new NotStoredException(topologyName + " isn't registered");
    }
  }

  @Override
  public synchronized TopologyStatus getTopologyStatus(String topologyId)
      throws MetaStoreException, NotStoredException {
    GungnirTopology topology = topologiesById.get(topologyId);
    if (topology != null) {
      return topology.getStatus();
    } else {
      throw new NotStoredException("Can't find topology '" + topologyId + "'");
    }
  }

  @Override
  public synchronized void deleteTopology(GungnirTopology topology) throws MetaStoreException,
      NotStoredException {
    if (topologiesById.remove(topology.getId()) != null) {
      topologiesByName.get(topology.getOwner().getId()).remove(topology.getName());

      LOG.info("Successful to delete topology {} owned by {}", topology.getName(),
          topology.getOwner().getName());
    } else {
      throw new NotStoredException("Can't find topology '" + topology.getId() + "'");
    }
  }

  @Override
  public synchronized String generateTrackingId() throws MetaStoreException {
    String id = generateUniqueId();
    trackingIds.put(id, ++currentTrackingId);
    LOG.info("Generated tracking id " + id + " with value " + currentTrackingId);
    return id;
  }

  @Override
  public synchronized Integer getTrackingNo(String tid) throws MetaStoreException,
      NotStoredException {
    Integer tno = trackingIds.get(tid);
    if (tno != null) {
      return tno;
    } else {
      throw new NotStoredException("Can't find tracking ID '" + tid + "'");
    }
  }

  private String generateUniqueId() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
