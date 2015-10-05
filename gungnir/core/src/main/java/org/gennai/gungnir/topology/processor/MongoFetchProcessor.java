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

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolder;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolders;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoFetchProcessor implements FetchProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(MongoFetchProcessor.class);

  private static final String FETCH_SERVERS = "mongo.fetch.servers";
  private static final String CACHE_SIZE = "mongo.fetch.cache.size";

  private static final Pattern ESCAPE_PATTERN = Pattern.compile("\\\\@");
  private static final Pattern NOT_ESCAPE_PATTERN =
      Pattern.compile("([:\\[,]?\\s*)@(\\w+(?:\\.\\w+)*)!?(\\s*[\\}\\],])");

  private String dbName;
  private String collectionName;
  private String queryString;
  private String[] fetchFieldNames;
  private String sortString;
  private Integer limit;
  private Period expire;
  private transient Map<String, Object> query;
  private transient Document fetchFields;
  private transient Document sort;
  private transient int expireSecs;
  private transient MongoClient mongoClient;
  private transient MongoCollection<Document> collection;
  private transient Cache<String, List<List<Object>>> cache;

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, String sortString, Integer limit, Period expire) {
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.queryString = queryString;
    this.fetchFieldNames = fetchFieldNames;
    this.sortString = sortString;
    this.limit = limit;
    this.expire = expire;
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames) {
    this(dbName, collectionName, queryString, fetchFieldNames, null, null, null);
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, String sortString) {
    this(dbName, collectionName, queryString, fetchFieldNames, sortString, null, null);
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, String sortString, Integer limit) {
    this(dbName, collectionName, queryString, fetchFieldNames, sortString, limit, null);
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, String sortString, Period expire) {
    this(dbName, collectionName, queryString, fetchFieldNames, sortString, null, expire);
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, Integer limit) {
    this(dbName, collectionName, queryString, fetchFieldNames, null, limit, null);
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, Integer limit, Period expire) {
    this(dbName, collectionName, queryString, fetchFieldNames, null, limit, expire);
  }

  public MongoFetchProcessor(String dbName, String collectionName, String queryString,
      String[] fetchFieldNames, Period expire) {
    this(dbName, collectionName, queryString, fetchFieldNames, null, null, expire);
  }

  private static Map<String, Object> toMongoQuery(Map<String, Object> queryMap) {
    Map<String, Object> query = Maps.newLinkedHashMap();
    for (Map.Entry<String, Object> entry : queryMap.entrySet()) {
      if (entry.getValue() instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) entry.getValue();
        query.put(entry.getKey(), toMongoQuery(map));
      } else if (entry.getValue() instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) entry.getValue();
        List<Object> qlist = Lists.newArrayList();
        for (Object element : list) {
          if (element instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) element;
            qlist.add(toMongoQuery(map));
          } else if (element instanceof String) {
            PlaceHolders placeHolders = ProcessorUtils.findPlaceHolders((String) element);
            if (placeHolders.isEmpty()) {
              qlist.add(placeHolders.getSrc());
            } else {
              qlist.add(placeHolders);
            }
          } else {
            qlist.add(element);
          }
        }
        query.put(entry.getKey(), qlist);
      } else if (entry.getValue() instanceof String) {
        PlaceHolders placeHolders = ProcessorUtils.findPlaceHolders((String) entry.getValue());
        if (placeHolders.isEmpty()) {
          query.put(entry.getKey(), placeHolders.getSrc());
        } else {
          query.put(entry.getKey(), placeHolders);
        }
      } else {
        query.put(entry.getKey(), entry.getValue());
      }
    }
    return query;
  }

  private static Map<String, Object> parseQueryString(String queryString)
      throws ProcessorException {
    Matcher matcher = NOT_ESCAPE_PATTERN.matcher(queryString);
    queryString = matcher.replaceAll("$1\"@$2!\"$3");
    matcher = ESCAPE_PATTERN.matcher(queryString);
    queryString = matcher.replaceAll("\\\\\\\\@");

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    Map<String, Object> queryMap = null;
    try {
      queryMap = mapper.readValue(queryString, mapper.getTypeFactory().constructMapType(
          LinkedHashMap.class, String.class, Object.class));
    } catch (IOException e) {
      throw new ProcessorException("Failed to parse query", e);
    }

    Map<String, Object> query = toMongoQuery(queryMap);
    return query;
  }

  private static void getGroupFields(Map<String, Object> query, Set<FieldAccessor> fields) {
    for (Map.Entry<String, Object> entry : query.entrySet()) {
      if (entry.getValue() instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) entry.getValue();
        getGroupFields(map, fields);
      } else if (entry.getValue() instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) entry.getValue();
        for (Object element : list) {
          if (element instanceof PlaceHolders) {
            for (PlaceHolder placeHolder : (PlaceHolders) element) {
              fields.add(placeHolder.getField());
            }
          }
        }
      } else if (entry.getValue() instanceof PlaceHolders) {
        for (PlaceHolder placeHolder : (PlaceHolders) entry.getValue()) {
          fields.add(placeHolder.getField());
        }
      }
    }
  }

  @Override
  public GroupFields getGroupFields() {
    if (query == null) {
      try {
        query = parseQueryString(queryString);
      } catch (ProcessorException e) {
        LOG.error("Failed to parse query", e);
        return null;
      }
    }

    Set<FieldAccessor> fields = Sets.newLinkedHashSet();
    getGroupFields(query, fields);
    if (fields.isEmpty()) {
      return null;
    }
    return new GroupFields(fields.toArray(new FieldAccessor[0]));
  }

  private static Document parseSortString(String sortString) throws ProcessorException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    Map<String, Object> sortMap = null;
    try {
      sortMap = mapper.readValue(sortString, mapper.getTypeFactory().constructMapType(
          LinkedHashMap.class, String.class, Object.class));
    } catch (IOException e) {
      throw new ProcessorException("Failed to parse sort", e);
    }

    return new Document(sortMap);
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context) throws ProcessorException {
    dbName = context.replaceVariable(dbName);
    collectionName = context.replaceVariable(collectionName);
    query = parseQueryString(queryString);
    fetchFields = new Document();
    for (String fieldName : fetchFieldNames) {
      fetchFields.append(fieldName, 1);
    }
    if (sortString != null) {
      sort = parseSortString(sortString);
    }
    if (expire != null) {
      expireSecs = expire.toSeconds();
    }

    List<String> servers = config.getList(FETCH_SERVERS);
    List<ServerAddress> addresses = Lists.newArrayListWithCapacity(servers.size());
    for (String server : servers) {
      addresses.add(new ServerAddress(server));
    }
    mongoClient = new MongoClient(addresses);
    MongoDatabase db = mongoClient.getDatabase(dbName);
    collection = db.getCollection(collectionName);

    if (expireSecs > 0) {
      cache = CacheBuilder.newBuilder().maximumSize(config.getInteger(CACHE_SIZE))
          .expireAfterWrite(expireSecs, TimeUnit.SECONDS).build();
    }

    LOG.info("MongoFetchProcessor opened({})", this);
  }

  private static Object getQueryValue(PlaceHolders placeHolders, GungnirTuple tuple) {
    if (placeHolders.size() == 1) {
      PlaceHolder placeHolder = placeHolders.iterator().next();
      if (placeHolder.getStart() == 0 && placeHolder.getEnd() == placeHolders.getSrc().length()) {
        return placeHolder.getField().getValue(tuple);
      }
    }

    StringBuilder sb = new StringBuilder();
    int start = 0;
    for (PlaceHolder placeHolder : placeHolders) {
      sb.append(placeHolders.getSrc().substring(start, placeHolder.getStart()));
      sb.append(placeHolder.getField().getValue(tuple));
      start = placeHolder.getEnd();
    }
    sb.append(placeHolders.getSrc().substring(start));
    return sb.toString();
  }

  private static Document getQuery(Map<String, Object> query, GungnirTuple tuple) {
    Document queryCopy = new Document();
    for (Map.Entry<String, Object> entry : query.entrySet()) {
      if (entry.getValue() instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) entry.getValue();
        queryCopy.append(entry.getKey(), getQuery(map, tuple));
      } else if (entry.getValue() instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) entry.getValue();
        BasicDBList dbList = new BasicDBList();
        for (Object element : list) {
          if (element instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) element;
            dbList.add(getQuery(map, tuple));
          } else if (element instanceof PlaceHolders) {
            dbList.add(getQueryValue((PlaceHolders) element, tuple));
          } else {
            dbList.add(element);
          }
          queryCopy.append(entry.getKey(), dbList);
        }
      } else if (entry.getValue() instanceof PlaceHolders) {
        queryCopy.append(entry.getKey(), getQueryValue((PlaceHolders) entry.getValue(), tuple));
      } else {
        queryCopy.append(entry.getKey(), entry.getValue());
      }
    }
    return queryCopy;
  }

  private static Object toValue(Object value) {
    if (value instanceof Document) {
      List<String> fieldNames = Lists.newArrayListWithCapacity(((Document) value).size());
      List<Object> values = Lists.newArrayListWithCapacity(((Document) value).size());
      for (Map.Entry<String, Object> entry : ((Document) value).entrySet()) {
        fieldNames.add(entry.getKey());
        values.add(toValue(entry.getValue()));
      }
      return new Struct(fieldNames, values);
    } else {
      return value;
    }
  }

  private List<List<Object>> find(Document execQuery) {
    List<List<Object>> valuesList = Lists.newArrayList();
    FindIterable<Document> find = collection.find(execQuery).projection(fetchFields);
    if (sort != null) {
      find.sort(sort);
    }
    if (limit != null) {
      find.limit(limit);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetch from '{}.{}' query {}", dbName, collectionName, execQuery);
    }

    MongoCursor<Document> cursor = find.iterator();
    try {
      while (cursor.hasNext()) {
        Document doc = cursor.next();

        List<Object> values = Lists.newArrayListWithCapacity(fetchFieldNames.length);
        for (String fieldName : fetchFieldNames) {
          values.add(toValue(doc.get(fieldName)));
        }
        valuesList.add(values);
      }
    } finally {
      cursor.close();
    }
    return valuesList;
  }

  @Override
  public List<List<Object>> fetch(GungnirTuple tuple) throws ProcessorException {
    if (collection == null) {
      throw new ProcessorException("Processor isn't open");
    }

    List<List<Object>> valuesList = null;
    try {
      final Document execQuery = getQuery(query, tuple);

      if (expireSecs > 0) {
        valuesList = cache.get(execQuery.toString(), new Callable<List<List<Object>>>() {

          @Override
          public List<List<Object>> call() throws Exception {
            return find(execQuery);
          }
        });
      } else {
        valuesList = find(execQuery);
      }

      return valuesList;
    } catch (ExecutionException e) {
      throw new ProcessorException("Failed to find document", e);
    } catch (MongoException e) {
      throw new ProcessorException("Failed to find document", e);
    }
  }

  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }

    LOG.info("MongoFetchProcessor closed({})", this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("mongo_fetch(");
    sb.append(dbName);
    sb.append(", ");
    sb.append(collectionName);
    sb.append(", ");
    sb.append(queryString);
    sb.append(", ");
    sb.append(Arrays.toString(fetchFieldNames));
    if (limit != null) {
      sb.append(", ");
      sb.append(limit);
    }
    if (expire != null) {
      sb.append(", ");
      sb.append(expire);
    }
    sb.append(')');
    return sb.toString();
  }
}
