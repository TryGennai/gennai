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

import static com.mongodb.client.model.Filters.*;
import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.ql.FileStat;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.FunctionEntity.FunctionType;
import org.gennai.gungnir.tuple.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoDbMetaStore implements MetaStore {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStore.class);

  private static final String USER_ACCOUNT_COLLECTION = "account";
  private static final String SCHEMA_COLLECTION = "schema";
  private static final String FILE_COLLECTION = "file";
  private static final String CHUNK_COLLECTION = "chunk";
  private static final String FUNCTION_COLLECTION = "function";
  private static final String TOPOLOGY_COLLECTION = "topology";
  private static final String TRACKING_COLLECTION = "tracking";

  private MongoClient mongoClient;
  private MongoDatabase metaStoreDB;
  private MongoCollection<Document> userAccountCollection;
  private MongoCollection<Document> schemaCollection;
  private MongoCollection<Document> fileCollection;
  private MongoCollection<Document> chunkCollection;
  private MongoCollection<Document> functionCollection;
  private MongoCollection<Document> topologyCollection;
  private MongoCollection<Document> trackingCollection;
  private Map<String, Integer> trackingMap;

  private void createIndexUserAccount() throws MetaStoreException {
    try {
      userAccountCollection.createIndex(new Document("name", 1), new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to ensure index of user account collection", e);
      throw new MetaStoreException("Failed to ensure index user account collection", e);
    }
  }

  private void createIndexSchema() throws MetaStoreException {
    try {
      schemaCollection.createIndex(new Document("name", 1).append("owner", 1),
          new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to create index of schema collection", e);
      throw new MetaStoreException("Failed to create index of schema collection", e);
    }
  }

  private void createIndexFile() throws MetaStoreException {
    try {
      fileCollection.createIndex(new Document("name", 1).append("owner", 1),
          new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to create index of file collection", e);
      throw new MetaStoreException("Failed to create index of file collection", e);
    }
  }

  private void createIndexChunk() throws MetaStoreException {
    try {
      chunkCollection.createIndex(new Document("fileId", 1));
    } catch (MongoException e) {
      LOG.error("Failed to create index of chunk collection", e);
      throw new MetaStoreException("Failed to create index of chunk collection", e);
    }
  }

  private void createIndexFunction() throws MetaStoreException {
    try {
      functionCollection.createIndex(new Document("name", 1).append("owner", 1),
          new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to create index of schema collection", e);
      throw new MetaStoreException("Failed to create index of schema collection", e);
    }
  }

  private void createIndexTopology() throws MetaStoreException {
    try {
      topologyCollection.createIndex(new Document("name", 1).append("owner", 1),
          new IndexOptions().unique(true));
    } catch (MongoException e) {
      LOG.error("Failed to create index of topology collection", e);
      throw new MetaStoreException("Failed to create index topology collection", e);
    }
  }

  @Override
  public void open() throws MetaStoreException {
    GungnirConfig config = GungnirManager.getManager().getConfig();
    List<String> servers = config.getList(METASTORE_MONGODB_SERVERS);
    List<ServerAddress> addresses = Lists.newArrayListWithCapacity(servers.size());
    for (String server : servers) {
      addresses.add(new ServerAddress(server));
    }
    mongoClient = new MongoClient(addresses);
    metaStoreDB = mongoClient.getDatabase(config.getString(METASTORE_NAME));
    userAccountCollection = metaStoreDB.getCollection(USER_ACCOUNT_COLLECTION);
    schemaCollection = metaStoreDB.getCollection(SCHEMA_COLLECTION);
    fileCollection = metaStoreDB.getCollection(FILE_COLLECTION);
    chunkCollection = metaStoreDB.getCollection(CHUNK_COLLECTION);
    functionCollection = metaStoreDB.getCollection(FUNCTION_COLLECTION);
    topologyCollection = metaStoreDB.getCollection(TOPOLOGY_COLLECTION);
    trackingCollection = metaStoreDB.getCollection(TRACKING_COLLECTION);

    trackingMap = Maps.newConcurrentMap();
  }

  @Override
  public void init() throws MetaStoreException {
    createIndexUserAccount();
    createIndexSchema();
    createIndexFile();
    createIndexChunk();
    createIndexFunction();
    createIndexTopology();

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

    try {
      createTrackingNoSequence();
    } catch (AlreadyStoredException ignore) {
      ignore = null;
    }
  }

  @Override
  public void drop() throws MetaStoreException {
    try {
      metaStoreDB.drop();
    } catch (MongoException e) {
      LOG.error("Failed to drop metastore", e);
      throw new MetaStoreException("Failed to drop metastore", e);
    }
  }

  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  @Override
  public void insertUserAccount(UserEntity user) throws MetaStoreException,
      AlreadyStoredException {
    try {
      Date createTime = new Date();
      Document doc = new Document("name", user.getName()).append("password", user.getPassword())
          .append("createTime", createTime).append("lastModifyTime", createTime);
      userAccountCollection.insertOne(doc);

      user.setId(doc.getObjectId("_id").toString());
      user.setCreateTime(createTime);

      LOG.info("Successful to insert user account '{}'", user.getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        // console out
        throw new AlreadyStoredException("'" + user.getName() + "' already exists");
      } else {
        LOG.error("Failed to insert user account", e);
        throw new MetaStoreException("Failed to insert user account", e);
      }
    }
  }

  public UserEntity findUserAccountById(String accountId) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(accountId)) {
      throw new MetaStoreException("Invalid account ID '" + accountId + "'");
    }

    try {
      Document doc = userAccountCollection.find(eq("_id", new ObjectId(accountId))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find user account '" + accountId + "'");
      }
      UserEntity user = new UserEntity();
      user.setId(doc.getObjectId("_id").toString());
      user.setName(doc.getString("name"));
      user.setPassword(doc.get("password", Binary.class).getData());
      user.setCreateTime(doc.getDate("createTime"));
      user.setLastModifyTime(doc.getDate("lastModifyTime"));
      return user;
    } catch (MongoException e) {
      LOG.error("Failed to find user account", e);
      throw new MetaStoreException("Failed to find user account", e);
    }
  }

  @Override
  public UserEntity findUserAccountByName(String userName) throws MetaStoreException,
      NotStoredException {
    try {
      Document doc = userAccountCollection.find(eq("name", userName)).first();
      if (doc == null) {
        // console out
        throw new NotStoredException("'" + userName + "' isn't registered");
      }
      UserEntity user = new UserEntity();
      user.setId(doc.getObjectId("_id").toString());
      user.setName(doc.getString("name"));
      user.setPassword(doc.get("password", Binary.class).getData());
      user.setCreateTime(doc.getDate("createTime"));
      user.setLastModifyTime(doc.getDate("lastModifyTime"));
      return user;
    } catch (MongoException e) {
      LOG.error("Failed to find user account", e);
      throw new MetaStoreException("Failed to find user account", e);
    }
  }

  @Override
  public List<UserEntity> findUserAccounts() throws MetaStoreException {
    try {
      List<UserEntity> users = Lists.newArrayList();

      MongoCursor<Document> cursor = userAccountCollection.find().iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          UserEntity user = new UserEntity();
          user.setId(doc.getObjectId("_id").toString());
          user.setName(doc.getString("name"));
          user.setPassword(doc.get("password", Binary.class).getData());
          user.setCreateTime(doc.getDate("createTime"));
          user.setLastModifyTime(doc.getDate("lastModifyTime"));

          users.add(user);
        }
      } finally {
        cursor.close();
      }

      return users;
    } catch (MongoException e) {
      LOG.error("Failed to find user accounts", e);
      throw new MetaStoreException("Failed to find user accounts", e);
    }
  }

  @Override
  public void updateUserAccount(UserEntity user) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(user.getId())) {
      throw new MetaStoreException("Invalid account ID '" + user.getId() + "'");
    }

    try {
      Date lastModifyTime = new Date();
      UpdateResult result = userAccountCollection.updateOne(eq("_id", new ObjectId(user.getId())),
          new Document("$set", new Document("name", user.getName())
              .append("password", user.getPassword()).append("lastModifyTime", lastModifyTime)));

      if (result.getModifiedCount() > 0) {
        user.setLastModifyTime(lastModifyTime);

        LOG.info("Successful to update user account '{}'", user.getName());
      } else {
        throw new NotStoredException("Can't find user account '" + user.getId() + "'");
      }
    } catch (MongoException e) {
      LOG.error("Failed to update user account", e);
      throw new MetaStoreException("Failed to update user account", e);
    }
  }

  @Override
  public void changeUserAccountPassword(UserEntity user) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(user.getId())) {
      throw new MetaStoreException("Invalid account ID '" + user.getId() + "'");
    }

    try {
      Date lastModifyTime = new Date();
      UpdateResult result = userAccountCollection.updateOne(eq("_id", new ObjectId(user.getId())),
          new Document("$set", new Document("password", user.getPassword())
              .append("lastModifyTime", lastModifyTime)));

      if (result.getModifiedCount() > 0) {
        user.setLastModifyTime(lastModifyTime);

        LOG.info("Successful to change password '{}'", user.getName());
      } else {
        throw new NotStoredException("Can't find user account '" + user.getId() + "'");
      }

      LOG.info("Successful to change password '{}'", user.getName());
    } catch (MongoException e) {
      LOG.error("Failed to change password", e);
      throw new MetaStoreException("Failed to change password", e);
    }
  }

  @Override
  public void deleteUserAccount(UserEntity user) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(user.getId())) {
      throw new MetaStoreException("Invalid account ID '" + user.getId() + "'");
    }

    try {
      DeleteResult result = userAccountCollection.deleteOne(eq("_id", new ObjectId(user.getId())));
      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete user account '{}'", user.getName());
      } else {
        throw new NotStoredException("Can't find user account '" + user.getId() + "'");
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete user account", e);
      throw new MetaStoreException("Failed to delete user account", e);
    }
  }

  @Override
  public void insertSchema(Schema schema) throws MetaStoreException, AlreadyStoredException {
    try {
      Date createTime = new Date();

      Document doc = new Document("name", schema.getSchemaName())
          .append("topologies", schema.getTopologies()).append("desc", Utils.serialize(schema))
          .append("owner", schema.getOwner().getId()).append("createTime", createTime);
      if (schema.getComment() != null) {
        doc.append("comment", schema.getComment());
      }
      schemaCollection.insertOne(doc);

      schema.setId(doc.getObjectId("_id").toString());
      schema.setCreateTime(createTime);

      LOG.info("Successful to insert schema {} owned by {}", schema.getSchemaName(),
          schema.getOwner().getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        // console out
        throw new AlreadyStoredException(schema.getSchemaName() + " already exists");
      } else {
        LOG.error("Failed to insert schema", e);
        throw new MetaStoreException("Failed to insert schema", e);
      }
    }
  }

  @Override
  public Schema findSchema(String schemaName, UserEntity owner) throws MetaStoreException,
      NotStoredException {
    try {
      Document doc = schemaCollection.find(and(eq("name", schemaName), eq("owner", owner.getId())))
          .first();
      if (doc == null) {
        // console out
        throw new NotStoredException(schemaName + " isn't registered");
      }

      Schema schema = (Schema) Utils.deserialize(doc.get("desc", Binary.class).getData());
      schema.setId(doc.getObjectId("_id").toString());
      @SuppressWarnings("unchecked")
      List<String> topologies = doc.get("topologies", ArrayList.class);
      schema.setTopologies(topologies);
      schema.setOwner(owner);
      schema.setCreateTime(doc.getDate("createTime"));
      schema.setComment(doc.getString("comment"));
      return schema;
    } catch (MongoException e) {
      LOG.error("Failed to find schema", e);
      throw new MetaStoreException("Failed to find schema", e);
    }
  }

  @Override
  public boolean changeSchemaToBusy(Schema schema, String topologyId) throws MetaStoreException {
    if (!ObjectId.isValid(schema.getId())) {
      throw new MetaStoreException("Invalid schema ID '" + schema.getId() + "'");
    }

    try {
      UpdateResult result = schemaCollection.updateOne(and(eq("_id", new ObjectId(schema.getId())),
          eq("createTime", schema.getCreateTime())),
          new Document("$push", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change status to busy {} owned by {}", schema.getSchemaName(),
            schema.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change status to busy", e);
      throw new MetaStoreException("Failed to change status to busy", e);
    }
  }

  @Override
  public boolean changeSchemaToFree(Schema schema, String topologyId) throws MetaStoreException {
    if (!ObjectId.isValid(schema.getId())) {
      throw new MetaStoreException("Invalid schema ID '" + schema.getId() + "'");
    }

    try {
      UpdateResult result = schemaCollection.updateOne(and(eq("_id", new ObjectId(schema.getId())),
          eq("createTime", schema.getCreateTime())),
          new Document("$pull", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change status to free {} owned by {}", schema.getSchemaName(),
            schema.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change status to free", e);
      throw new MetaStoreException("Failed to change status to free", e);
    }
  }

  @Override
  public List<Schema> findSchemas(UserEntity owner) throws MetaStoreException {
    try {
      List<Schema> schemas = Lists.newArrayList();

      MongoCursor<Document> cursor = schemaCollection.find(eq("owner", owner.getId())).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();

          Schema schema = (Schema) Utils.deserialize(doc.get("desc", Binary.class).getData());
          schema.setId(doc.getObjectId("_id").toString());
          @SuppressWarnings("unchecked")
          List<String> topologies = doc.get("topologies", ArrayList.class);
          schema.setTopologies(topologies);
          schema.setOwner(owner);
          schema.setCreateTime(doc.getDate("createTime"));
          schema.setComment(doc.getString("comment"));

          schemas.add(schema);
        }
      } finally {
        cursor.close();
      }

      return schemas;
    } catch (MongoException e) {
      LOG.error("Failed to find schemas", e);
      throw new MetaStoreException("Failed to find schemas", e);
    }
  }

  @Override
  public boolean deleteSchema(Schema schema) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(schema.getId())) {
      throw new MetaStoreException("Invalid schema ID '" + schema.getId() + "'");
    }

    try {
      DeleteResult result = schemaCollection.deleteOne(and(eq("_id", new ObjectId(schema.getId())),
          size("topologies", 0)));
      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete schema {} owned by {}", schema.getSchemaName(),
            schema.getOwner().getName());
        return true;
      } else {
        if (schemaCollection.count(eq("_id", new ObjectId(schema.getId()))) == 0) {
          throw new NotStoredException("Can't find schema '" + schema.getId() + "'");
        }
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete schema", e);
      throw new MetaStoreException("Failed to delete schema", e);
    }
  }

  @Override
  public void insertFile(FileStat fileStat) throws MetaStoreException, AlreadyStoredException {
    try {
      Date createTime = new Date();

      Document doc =
          new Document("name", fileStat.getName()).append("size", fileStat.getSize())
              .append("checksum", fileStat.getChecksum())
              .append("topologies", fileStat.getTopologies())
              .append("owner", fileStat.getOwner().getId()).append("createTime", createTime);
      if (fileStat.getComment() != null) {
        doc.append("comment", fileStat.getComment());
      }
      fileCollection.insertOne(doc);

      fileStat.setId(doc.getObjectId("_id").toString());
      fileStat.setCreateTime(createTime);

      LOG.info("Successful to insert file '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        // console out
        throw new AlreadyStoredException("'" + fileStat.getName() + "' already exists");
      } else {
        LOG.error("Failed to insert file", e);
        throw new MetaStoreException("Failed to insert file", e);
      }
    }
  }

  @Override
  public void changeFileStat(FileStat fileStat) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(fileStat.getId())) {
      throw new MetaStoreException("Invalid file ID '" + fileStat.getId() + "'");
    }

    try {
      Date createTime = new Date();

      UpdateResult result = fileCollection.updateOne(eq("_id", new ObjectId(fileStat.getId())),
          new Document("$set", new Document("size", fileStat.getSize())
              .append("checksum", fileStat.getChecksum()).append("createTime", createTime)));

      if (result.getModifiedCount() > 0) {
        fileStat.setCreateTime(createTime);

        LOG.info("Successful to update stat '{}' owned by {}", fileStat.getName(),
            fileStat.getOwner().getName());
      } else {
        throw new NotStoredException("Can't find file '" + fileStat.getName() + "'");
      }
    } catch (MongoException e) {
      LOG.error("Failed to update stat", e);
      throw new MetaStoreException("Failed to update stat", e);
    }
  }

  @Override
  public FileStat findFile(String fileName, UserEntity owner) throws MetaStoreException,
      NotStoredException {
    try {
      Document doc = fileCollection.find(and(eq("name", fileName), eq("owner", owner.getId())))
          .first();
      if (doc == null) {
        // console out
        throw new NotStoredException("'" + fileName + "' isn't registered");
      }
      FileStat fileStat = new FileStat();
      fileStat.setId(doc.getObjectId("_id").toString());
      fileStat.setName(doc.getString("name"));
      fileStat.setSize(doc.getInteger("size"));
      fileStat.setChecksum(doc.getLong("checksum"));
      @SuppressWarnings("unchecked")
      List<String> topologies = doc.get("topologies", ArrayList.class);
      fileStat.setTopologies(topologies);
      fileStat.setOwner(owner);
      fileStat.setCreateTime(doc.getDate("createTime"));
      fileStat.setComment(doc.getString("comment"));
      return fileStat;
    } catch (MongoException e) {
      LOG.error("Failed to find file", e);
      throw new MetaStoreException("Failed to find file", e);
    }
  }

  @Override
  public boolean changeFileToBusy(FileStat fileStat, String topologyId) throws MetaStoreException {
    if (!ObjectId.isValid(fileStat.getId())) {
      throw new MetaStoreException("Invalid file ID '" + fileStat.getId() + "'");
    }

    try {
      UpdateResult result = fileCollection.updateOne(and(eq("_id", new ObjectId(fileStat.getId())),
          eq("createTime", fileStat.getCreateTime())),
          new Document("$push", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change stat to busy '{}' owned by {}", fileStat.getName(),
            fileStat.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change stat to busy", e);
      throw new MetaStoreException("Failed to change stat to busy", e);
    }
  }

  @Override
  public boolean changeFileToFree(FileStat fileStat, String topologyId) throws MetaStoreException {
    if (!ObjectId.isValid(fileStat.getId())) {
      throw new MetaStoreException("Invalid schema ID '" + fileStat.getId() + "'");
    }

    try {
      UpdateResult result = fileCollection.updateOne(and(eq("_id", new ObjectId(fileStat.getId())),
          eq("createTime", fileStat.getCreateTime())),
          new Document("$pull", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change stat to free '{}' owned by {}", fileStat.getName(),
            fileStat.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change stat to free", e);
      throw new MetaStoreException("Failed to change stat to free", e);
    }
  }

  @Override
  public List<FileStat> findFiles(UserEntity owner) throws MetaStoreException {
    try {
      List<FileStat> files = Lists.newArrayList();

      MongoCursor<Document> cursor = fileCollection.find(eq("owner", owner.getId())).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          FileStat fileStat = new FileStat();
          fileStat.setId(doc.getObjectId("_id").toString());
          fileStat.setName(doc.getString("name"));
          fileStat.setSize(doc.getInteger("size"));
          fileStat.setChecksum(doc.getLong("checksum"));
          @SuppressWarnings("unchecked")
          List<String> topologies = doc.get("topologies", ArrayList.class);
          fileStat.setTopologies(topologies);
          fileStat.setOwner(owner);
          fileStat.setCreateTime(doc.getDate("createTime"));
          fileStat.setComment(doc.getString("comment"));

          files.add(fileStat);
        }
      } finally {
        cursor.close();
      }

      return files;
    } catch (MongoException e) {
      LOG.error("Failed to find files", e);
      throw new MetaStoreException("Failed to find files", e);
    }
  }

  @Override
  public boolean deleteFile(FileStat fileStat) throws MetaStoreException, NotStoredException {
    if (!ObjectId.isValid(fileStat.getId())) {
      throw new MetaStoreException("Invalid file ID '" + fileStat.getId() + "'");
    }

    try {
      DeleteResult result = fileCollection.deleteOne(and(eq("_id", new ObjectId(fileStat.getId())),
          gt("size", 0), size("topologies", 0)));
      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete file '{}' owned by {}", fileStat.getName(),
            fileStat.getOwner().getName());
        return true;
      } else {
        if (fileCollection.count(eq("_id", new ObjectId(fileStat.getId()))) == 0) {
          throw new NotStoredException("Can't find file '" + fileStat.getId() + "'");
        }
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete file", e);
      throw new MetaStoreException("Failed to delete file", e);
    }
  }

  @Override
  public void insertChunk(FileStat fileStat, byte[] chunk) throws MetaStoreException {
    try {
      Document doc = new Document("fileId", fileStat.getId()).append("chunk", chunk);
      chunkCollection.insertOne(doc);

      LOG.info("Successful to insert chunk '{}' owned by {}", fileStat.getName(),
          fileStat.getOwner().getName());
    } catch (MongoException e) {
      LOG.error("Failed to insert chunk", e);
      throw new MetaStoreException("Failed to insert chunk", e);
    }
  }

  public static class MongoChunkIterator implements ChunkIterator {

    private MongoCursor<Document> cursor;

    public MongoChunkIterator(MongoCursor<Document> cursor) {
      this.cursor = cursor;
    }

    @Override
    public boolean hasNext() {
      return cursor.hasNext();
    }

    @Override
    public byte[] next() {
      Document doc = cursor.next();
      return doc.get("chunk", Binary.class).getData();
    }

    @Override
    public void close() {
      cursor.close();
    }
  }

  @Override
  public ChunkIterator findChunks(FileStat fileStat) throws MetaStoreException {
    try {
      MongoCursor<Document> cursor = chunkCollection.find(eq("fileId", fileStat.getId()))
          .iterator();
      return new MongoChunkIterator(cursor);
    } catch (MongoException e) {
      LOG.error("Failed to find files", e);
      throw new MetaStoreException("Failed to find files", e);
    }
  }

  @Override
  public boolean exportFile(FileStat fileStat, String exportDir, int retryTimes, int retryInterval)
      throws MetaStoreException, NotStoredException {
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

      ChunkIterator it = null;
      SeekableByteChannel sbc = null;
      try {
        Path path = Paths.get(exportDir);
        if (!Files.exists(path)) {
          Files.createDirectories(path);
        }
        sbc = Files.newByteChannel(Paths.get(exportDir, fileStat.getName()),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        for (it = findChunks(fileStat); it.hasNext();) {
          sbc.write(ByteBuffer.wrap(it.next()));
        }
      } catch (IOException e) {
        throw new MetaStoreException("Failed to export file", e);
      } finally {
        if (it != null) {
          it.close();
        }
        if (sbc != null) {
          try {
            sbc.close();
          } catch (IOException e) {
            throw new MetaStoreException("Failed to export file", e);
          }
        }
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
  public void deleteChunks(FileStat fileStat) throws MetaStoreException, NotStoredException {
    try {
      DeleteResult result = chunkCollection.deleteMany(eq("fileId", fileStat.getId()));
      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete chunks '{}'", fileStat.getId());
      } else {
        throw new NotStoredException("Can't find chunks '" + fileStat.getId() + "'");
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete chunks", e);
      throw new MetaStoreException("Failed to delete chunks", e);
    }
  }

  @Override
  public void insertFunction(FunctionEntity function) throws MetaStoreException,
      AlreadyStoredException {
    try {
      Date createTime = new Date();

      Document doc =
          new Document("name", function.getName()).append("type", function.getType().toString())
              .append("location", function.getLocation())
              .append("topologies", function.getTopologies())
              .append("owner", function.getOwner().getId()).append("createTime", createTime);
      if (function.getComment() != null) {
        doc.append("comment", function.getComment());
      }
      functionCollection.insertOne(doc);

      function.setId(doc.getObjectId("_id").toString());
      function.setCreateTime(createTime);

      LOG.info("Successful to insert function {} owned by {}", function.getName(),
          function.getOwner().getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        // console out
        throw new AlreadyStoredException(function.getName() + " already exists");
      } else {
        LOG.error("Failed to insert function", e);
        throw new MetaStoreException("Failed to insert function", e);
      }
    }
  }

  @Override
  public FunctionEntity findFunction(String functionName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    try {
      Document doc = functionCollection.find(
          and(eq("name", functionName), eq("owner", owner.getId()))).first();
      if (doc == null) {
        // console out
        throw new NotStoredException(functionName + " isn't registered");
      }
      FunctionEntity function = new FunctionEntity();
      function.setId(doc.getObjectId("_id").toString());
      function.setName(doc.getString("name"));
      function.setType(FunctionType.valueOf(doc.getString("type")));
      function.setLocation(doc.getString("location"));
      @SuppressWarnings("unchecked")
      List<String> topologies = doc.get("topologies", ArrayList.class);
      function.setTopologies(topologies);
      function.setOwner(owner);
      function.setCreateTime(doc.getDate("createTime"));
      function.setComment(doc.getString("comment"));
      return function;
    } catch (MongoException e) {
      LOG.error("Failed to find function", e);
      throw new MetaStoreException("Failed to find function", e);
    }
  }

  @Override
  public boolean changeFunctionToBusy(FunctionEntity function, String topologyId)
      throws MetaStoreException {
    if (!ObjectId.isValid(function.getId())) {
      throw new MetaStoreException("Invalid function ID '" + function.getId() + "'");
    }

    try {
      UpdateResult result = functionCollection.updateOne(
          and(eq("_id", new ObjectId(function.getId())),
              eq("createTime", function.getCreateTime())),
          new Document("$push", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change status to busy {} owned by {}", function.getName(),
            function.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change status to busy", e);
      throw new MetaStoreException("Failed to change status to busy", e);
    }
  }

  @Override
  public boolean changeFunctionToFree(FunctionEntity function, String topologyId)
      throws MetaStoreException {
    if (!ObjectId.isValid(function.getId())) {
      throw new MetaStoreException("Invalid function ID '" + function.getId() + "'");
    }

    try {
      UpdateResult result = functionCollection.updateOne(
          and(eq("_id", new ObjectId(function.getId())),
              eq("createTime", function.getCreateTime())),
          new Document("$pull", new Document("topologies", topologyId)));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change status to free {} owned by {}", function.getName(),
            function.getOwner().getName());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change status to free", e);
      throw new MetaStoreException("Failed to change status to free", e);
    }
  }

  @Override
  public List<FunctionEntity> findFunctions(UserEntity owner) throws MetaStoreException {
    try {
      List<FunctionEntity> functions = Lists.newArrayList();

      MongoCursor<Document> cursor = functionCollection.find(eq("owner", owner.getId())).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          FunctionEntity function = new FunctionEntity();
          function.setId(doc.getObjectId("_id").toString());
          function.setName(doc.getString("name"));
          function.setType(FunctionType.valueOf(doc.getString("type")));
          function.setLocation(doc.getString("location"));
          @SuppressWarnings("unchecked")
          List<String> topologies = doc.get("topologies", ArrayList.class);
          function.setTopologies(topologies);
          function.setOwner(owner);
          function.setCreateTime(doc.getDate("createTime"));
          function.setComment(doc.getString("comment"));

          functions.add(function);
        }
      } finally {
        cursor.close();
      }

      return functions;
    } catch (MongoException e) {
      LOG.error("Failed to find functions", e);
      throw new MetaStoreException("Failed to find functions", e);
    }
  }

  @Override
  public boolean deleteFunction(FunctionEntity function) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(function.getId())) {
      throw new MetaStoreException("Invalid function ID '" + function.getId() + "'");
    }

    try {
      DeleteResult result = functionCollection.deleteOne(
          and(eq("_id", new ObjectId(function.getId())), size("topologies", 0)));
      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete function {} owned by {}", function.getName(),
            function.getOwner().getName());
        return true;
      } else {
        if (functionCollection.count(eq("_id", new ObjectId(function.getId()))) == 0) {
          throw new NotStoredException("Can't find function '" + function.getId() + "'");
        }
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete function", e);
      throw new MetaStoreException("Failed to delete function", e);
    }
  }

  @Override
  public void insertTopology(GungnirTopology topology) throws MetaStoreException,
      AlreadyStoredException {
    try {
      Date createTime = new Date();

      Document doc = new Document("name", topology.getName())
          .append("status", topology.getStatus().toString())
          .append("desc", Utils.serialize(topology)).append("owner", topology.getOwner().getId())
          .append("createTime", createTime);
      if (topology.getComment() != null) {
        doc.append("comment", topology.getComment());
      }
      topologyCollection.insertOne(doc);

      topology.setId(doc.getObjectId("_id").toString());
      topology.setCreateTime(createTime);

      LOG.info("Successful to insert topology {}  owned by {}", topology.getName(),
          topology.getOwner().getName());
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        // console out
        throw new AlreadyStoredException(topology.getName() + " already exists");
      } else {
        LOG.error("Failed to insert topology", e);
        throw new MetaStoreException("Failed to insert topology", e);
      }
    }
  }

  @Override
  public boolean changeTopologyStatus(GungnirTopology topology) throws MetaStoreException {
    if (!ObjectId.isValid(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }

    try {
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

      UpdateResult result = topologyCollection.updateOne(
          and(eq("_id", new ObjectId(topology.getId())), eq("status", status.toString())),
          new Document("$set", new Document("status", topology.getStatus().toString())));

      if (result.getModifiedCount() > 0) {
        LOG.info("Successful to change topology status '{}' ({}->{})", topology.getId(), status,
            topology.getStatus());
        return true;
      } else {
        return false;
      }
    } catch (MongoException e) {
      LOG.error("Failed to change topology status", e);
      throw new MetaStoreException("Failed to change topology status", e);
    }
  }

  @Override
  public void changeForcedTopologyStatus(GungnirTopology topology) throws MetaStoreException {
    if (!ObjectId.isValid(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }

    try {
      topologyCollection.updateOne(eq("_id", new ObjectId(topology.getId())),
          new Document("$set", new Document("status", topology.getStatus().toString())));
      LOG.info("Successful to change topology status '{}' ({})", topology.getId(),
          topology.getStatus());
    } catch (MongoException e) {
      LOG.error("Failed to change topology status", e);
      throw new MetaStoreException("Failed to change topology status", e);
    }
  }

  @Override
  public List<GungnirTopology> findTopologies(UserEntity owner) throws MetaStoreException {
    try {
      List<GungnirTopology> topologies = Lists.newArrayList();

      MongoCursor<Document> cursor = topologyCollection.find(eq("owner", owner.getId())).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          GungnirTopology topology = (GungnirTopology) Utils.deserialize(
              doc.get("desc", Binary.class).getData());
          topology.setId(doc.getObjectId("_id").toString());
          topology.setName(doc.getString("name"));
          topology.setOwner(owner);
          topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
          topology.setCreateTime(doc.getDate("createTime"));
          topology.setComment(doc.getString("comment"));

          topologies.add(topology);
        }
      } finally {
        cursor.close();
      }

      return topologies;
    } catch (MongoException e) {
      LOG.error("Failed to find topologies", e);
      throw new MetaStoreException("Failed to find topologies", e);
    }
  }

  @Override
  public List<GungnirTopology> findTopologies(UserEntity owner, TopologyStatus status)
      throws MetaStoreException {
    try {
      List<GungnirTopology> topologies = Lists.newArrayList();

      MongoCursor<Document> cursor = topologyCollection.find(
          and(eq("owner", owner.getId()), eq("status", status.toString()))).iterator();
      try {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          GungnirTopology topology = (GungnirTopology) Utils.deserialize(
              doc.get("desc", Binary.class).getData());
          topology.setId(doc.getObjectId("_id").toString());
          topology.setName(doc.getString("name"));
          topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
          topology.setOwner(owner);
          topology.setCreateTime(doc.getDate("createTime"));
          topology.setComment(doc.getString("comment"));

          topologies.add(topology);
        }
      } finally {
        cursor.close();
      }

      return topologies;
    } catch (MongoException e) {
      LOG.error("Failed to find topologies", e);
      throw new MetaStoreException("Failed to find topologies", e);
    }
  }

  @Override
  public GungnirTopology findTopologyById(String topologyId) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(topologyId)) {
      throw new MetaStoreException("Invalid topology ID '" + topologyId + "'");
    }

    try {
      Document doc = topologyCollection.find(eq("_id", new ObjectId(topologyId))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find topology '" + topologyId + "'");
      }

      GungnirTopology topology = (GungnirTopology) Utils.deserialize(
          doc.get("desc", Binary.class).getData());
      topology.setId(doc.getObjectId("_id").toString());
      topology.setName(doc.getString("name"));
      topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
      topology.setOwner(findUserAccountById(doc.getString("owner")));
      topology.setCreateTime(doc.getDate("createTime"));
      topology.setComment(doc.getString("comment"));
      return topology;
    } catch (MongoException e) {
      LOG.error("Failed to find topology", e);
      throw new MetaStoreException("Failed to find topology", e);
    }
  }

  @Override
  public GungnirTopology findTopologyByName(String topologyName, UserEntity owner)
      throws MetaStoreException, NotStoredException {
    try {
      Document doc = topologyCollection.find(
          and(eq("name", topologyName), eq("owner", owner.getId()))).first();
      if (doc == null) {
        // console out
        throw new NotStoredException(topologyName + " isn't registered");
      }

      GungnirTopology topology = (GungnirTopology) Utils.deserialize(
          doc.get("desc", Binary.class).getData());
      topology.setId(doc.getObjectId("_id").toString());
      topology.setName(doc.getString("name"));
      topology.setStatus(TopologyStatus.valueOf(doc.getString("status")));
      topology.setOwner(owner);
      topology.setCreateTime(doc.getDate("createTime"));
      topology.setComment(doc.getString("comment"));
      return topology;
    } catch (MongoException e) {
      LOG.error("Failed to find topology", e);
      throw new MetaStoreException("Failed to find topology", e);
    }
  }

  @Override
  public TopologyStatus getTopologyStatus(String topologyId) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(topologyId)) {
      throw new MetaStoreException("Invalid topology ID '" + topologyId + "'");
    }

    try {
      Document doc = topologyCollection.find(
          eq("_id", new ObjectId(topologyId))).projection(new Document("status", 1)).first();
      if (doc == null) {
        throw new NotStoredException("Can't find topology '" + topologyId + "'");
      }
      return TopologyStatus.valueOf(doc.getString("status"));
    } catch (MongoException e) {
      LOG.error("Failed to get topology status", e);
      throw new MetaStoreException("Failed to get topology status", e);
    }
  }

  @Override
  public void deleteTopology(GungnirTopology topology) throws MetaStoreException,
      NotStoredException {
    if (!ObjectId.isValid(topology.getId())) {
      throw new MetaStoreException("Invalid topology ID '" + topology.getId() + "'");
    }

    try {
      DeleteResult result = topologyCollection.deleteOne(eq("_id", new ObjectId(topology.getId())));
      if (result.getDeletedCount() > 0) {
        LOG.info("Successful to delete topology {} owned by {}", topology.getName(),
            topology.getOwner().getName());
      } else {
        throw new NotStoredException("Can't find topology '" + topology.getId() + "'");
      }
    } catch (MongoException e) {
      LOG.error("Failed to delete topology", e);
      throw new MetaStoreException("Failed to delete topology", e);
    }
  }

  private void createTrackingNoSequence() throws MetaStoreException, AlreadyStoredException {
    try {
      trackingCollection.insertOne(new Document("_id", "_tno").append("sequence", 0));
      LOG.info("Successful to create tracking no sequence");
    } catch (MongoException e) {
      if (e.getCode() == 11000) {
        throw new AlreadyStoredException("Tracking no sequence already exists");
      } else {
        LOG.error("Failed to create tracking no sequence", e);
        throw new MetaStoreException("Failed to create tracking no sequence", e);
      }
    }
  }

  @Override
  public String generateTrackingId() throws MetaStoreException {
    try {
      Document seq = trackingCollection.findOneAndUpdate(eq("_id", "_tno"),
          new Document("$inc", new Document("sequence", 1)));
      Integer tno = seq.getInteger("sequence");
      Date createTime = new Date();
      Document doc = new Document("no", tno).append("createTime", createTime);
      trackingCollection.insertOne(doc);
      String tid = doc.getObjectId("_id").toString();

      trackingMap.put(tid, tno);

      LOG.info("Successful to generate tracking ID '{}', tracking No {}", tid, tno);
      return tid;
    } catch (MongoException e) {
      LOG.error("Failed to generate UUID", e);
      throw new MetaStoreException("Failed to generate tracking ID", e);
    }
  }

  @Override
  public Integer getTrackingNo(String tid) throws MetaStoreException, NotStoredException {
    Integer tno = trackingMap.get(tid);
    if (tno != null) {
      return tno;
    }

    if (!ObjectId.isValid(tid)) {
      throw new MetaStoreException("Invalid tracking ID '" + tid + "'");
    }

    try {
      Document doc = trackingCollection.find(eq("_id", new ObjectId(tid))).first();
      if (doc == null) {
        throw new NotStoredException("Can't find tracking ID '" + tid + "'");
      }

      tno = doc.getInteger("no");
      trackingMap.put(tid, tno);
      return tno;
    } catch (MongoException e) {
      LOG.error("Failed to find tracking ID", e);
      throw new MetaStoreException("Failed to find tracking ID", e);
    }
  }
}
