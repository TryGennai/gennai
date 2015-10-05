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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

public class MongoPersistProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(MongoPersistProcessor.class);

  private static final String MONGO_PERSIST_SERVERS = "mongo.persist.servers";

  private String dbName;
  private String collectionName;
  private boolean autoIndexing;
  private String[] keyFieldNames;
  private transient Map<String, List<String>> outputFieldNames;
  private transient Map<String, int[]> keyFieldsIndexes;
  private transient MongoClient mongoClient;
  private transient MongoCollection<Document> collection;

  public MongoPersistProcessor(String dbName, String collectionName) {
    this.dbName = dbName;
    this.collectionName = collectionName;
  }

  public MongoPersistProcessor(String dbName, String collectionName, boolean autoIndexing,
      String... keyFieldNames) {
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.autoIndexing = autoIndexing;
    this.keyFieldNames = keyFieldNames;
  }

  public MongoPersistProcessor(String dbName, String collectionName, String keyFieldName) {
    this(dbName, collectionName, false, new String[] {keyFieldName});
  }

  public MongoPersistProcessor(String dbName, String collectionName, String... keyFieldNames) {
    this(dbName, collectionName, false, keyFieldNames);
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    dbName = context.replaceVariable(dbName);
    collectionName = context.replaceVariable(collectionName);
    this.outputFieldNames = outputFieldNames;

    if (keyFieldNames != null) {
      keyFieldsIndexes = Maps.newHashMap();
      for (Map.Entry<String, List<String>> entry : outputFieldNames.entrySet()) {
        int[] index = new int[keyFieldNames.length];
        Arrays.fill(index, -1);
        for (int i = 0; i < keyFieldNames.length; i++) {
          for (int j = 0; j < entry.getValue().size(); j++) {
            if (entry.getValue().get(j).equals(keyFieldNames[i])) {
              index[i] = j;
              break;
            }
          }
          if (index[i] < 0) {
            throw new ProcessorException("Can't found key field '" + keyFieldNames[i] + "'");
          }
        }
        keyFieldsIndexes.put(entry.getKey(), index);
      }
    }

    List<String> servers = config.getList(MONGO_PERSIST_SERVERS);
    List<ServerAddress> addresses = Lists.newArrayListWithCapacity(servers.size());
    for (String server : servers) {
      addresses.add(new ServerAddress(server));
    }
    mongoClient = new MongoClient(addresses);
    MongoDatabase db = mongoClient.getDatabase(dbName);
    collection = db.getCollection(collectionName);

    if (autoIndexing && keyFieldNames != null) {
      Document doc = new Document();
      for (String keyFieldName : keyFieldNames) {
        doc.append(keyFieldName, 1);
      }

      collection.createIndex(doc, new IndexOptions().unique(true));
    }

    LOG.info("MongoPersistProcessor opened({})", this);
  }

  private static Document toDBObject(List<String> fieldNames, List<Object> values) {
    Document doc = new Document();
    for (int i = 0; i < fieldNames.size(); i++) {
      Object value = values.get(i);
      if (value instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        Document mapDoc = new Document();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          // TODO
          /*
          if (entry.getKey() instanceof Double) {
            mapDoc.put(
                "0x" + Long.toHexString(Double.doubleToLongBits((Double) entry.getKey())),
                entry.getValue());
          } else if (entry.getKey() instanceof Float) {
            mapDoc.put("0x" + Integer.toHexString(Float.floatToIntBits((Float) entry.getKey())),
                entry.getValue());
          } else {
          */
            mapDoc.put(entry.getKey().toString(), entry.getValue());
          //}
        }
        value = mapDoc;
      } else if (value instanceof Struct) {
        value = toDBObject(((Struct) value).getFieldNames(), ((Struct) value).getValues());
      }
      doc.append(fieldNames.get(i), value);
    }
    return doc;
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    if (collection == null) {
      throw new ProcessorException("Processor isn't open");
    }

    if (keyFieldNames == null) {
      List<Document> list = Lists.newArrayListWithCapacity(tuples.size());
      for (TupleValues tupleValues : tuples) {
        List<String> fieldNames = outputFieldNames.get(tupleValues.getTupleName());
        if (fieldNames.size() > 0) {
          Document doc = toDBObject(fieldNames, tupleValues.getValues());
          list.add(doc);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Insert into {}.{} values {}", dbName, collectionName, doc);
          }
        }
      }

      if (list.size() > 0) {
        try {
          collection.insertMany(list);
        } catch (MongoException e) {
          LOG.error("Failed to insert documents", e);
        }
      }
    } else {
      for (TupleValues tupleValues : tuples) {
        List<String> fieldNames = outputFieldNames.get(tupleValues.getTupleName());
        if (!fieldNames.isEmpty()) {
          BasicDBObject query = null;
          if (keyFieldsIndexes != null) {
            int[] index = keyFieldsIndexes.get(tupleValues.getTupleName());
            query = new BasicDBObject();
            for (int i = 0; i < index.length; i++) {
              query.append(keyFieldNames[i], tupleValues.getValues().get(index[i]));
            }
          }

          Document doc = toDBObject(fieldNames, tupleValues.getValues());
          try {
            collection.updateOne(query, new Document("$set", doc),
                new UpdateOptions().upsert(true));

            if (LOG.isDebugEnabled()) {
              LOG.debug("Update '{}.{}' set {} where {}", dbName, collectionName, doc, query);
            }
          } catch (MongoException e) {
            LOG.error("Failed to update documents", e);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }

    LOG.info("MongoPersistProcessor closed({})", this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("mongo_persist(");
    sb.append(dbName);
    sb.append(", ");
    sb.append(collectionName);
    if (keyFieldNames != null) {
      if (autoIndexing) {
        sb.append(", true");
      }
      sb.append(", ");
      sb.append(Arrays.toString(keyFieldNames));
    }
    sb.append(')');
    return sb.toString();
  }
}
