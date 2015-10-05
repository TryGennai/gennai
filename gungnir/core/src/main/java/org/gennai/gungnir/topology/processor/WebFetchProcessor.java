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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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

import org.apache.commons.io.Charsets;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolder;
import org.gennai.gungnir.topology.processor.ProcessorUtils.PlaceHolders;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class WebFetchProcessor implements FetchProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(WebFetchProcessor.class);

  private static final String CACHE_SIZE = "web.fetch.cache.size";

  private static final Pattern RESPONSE_PATH_PATTERN = Pattern.compile("^(.+)\\[(\\d+)\\]$");

  private String url;
  private String rootPathString;
  private String[] fetchFieldNames;
  private Period expire;
  private transient PlaceHolders placeHolders;
  private transient List<Object> rootPath;
  private transient int expireSecs;
  private transient CloseableHttpClient client;
  private transient ObjectMapper mapper;
  private transient Cache<String, List<List<Object>>> cache;

  public WebFetchProcessor(String url, String rootPathString, String[] fetchFieldNames,
      Period expire) {
    this.url = url;
    this.rootPathString = rootPathString;
    this.fetchFieldNames = fetchFieldNames;
    this.expire = expire;
  }

  public WebFetchProcessor(String url, String rootPathString, String[] fetchFieldNames) {
    this(url, rootPathString, fetchFieldNames, null);
  }

  @Override
  public GroupFields getGroupFields() {
    if (placeHolders == null) {
      placeHolders = ProcessorUtils.findPlaceHolders(url);
    }

    if (placeHolders.isEmpty()) {
      return null;
    }
    Set<FieldAccessor> fields = Sets.newLinkedHashSet();
    for (PlaceHolder placeHolder : placeHolders) {
      fields.add(placeHolder.getField());
    }
    return new GroupFields(fields.toArray(new FieldAccessor[0]));
  }

  private static List<Object> parseRootPath(String rootPathString) {
    List<Object> rootPath = Lists.newArrayList();
    String[] paths = rootPathString.split("\\.");
    if (paths.length > 0) {
      for (int i = 0; i < paths.length; i++) {
        Matcher matcher = RESPONSE_PATH_PATTERN.matcher(paths[i]);
        if (matcher.find()) {
          rootPath.add(matcher.group(1));
          rootPath.add(Integer.parseInt(matcher.group(2)));
        } else {
          rootPath.add(paths[i]);
        }
      }
    }
    return rootPath;
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context) throws ProcessorException {
    placeHolders = ProcessorUtils.findPlaceHolders(url);
    rootPath = parseRootPath(rootPathString);
    if (expire != null) {
      expireSecs = expire.toSeconds();
    }

    client = HttpClientBuilder.create().build();
    mapper = new ObjectMapper();

    cache = CacheBuilder.newBuilder().maximumSize(config.getInteger(CACHE_SIZE))
        .expireAfterWrite(expireSecs, TimeUnit.SECONDS).build();

    LOG.info("WebFetchProcessor opened({})", this);
  }

  private static URI getRequestUri(PlaceHolders placeHolders, GungnirTuple tuple)
      throws MalformedURLException, URISyntaxException {
    if (placeHolders.isEmpty()) {
      return new URI(placeHolders.getSrc());
    }

    StringBuilder sb = new StringBuilder();
    int start = 0;
    for (PlaceHolder placeHolder : placeHolders) {
      sb.append(placeHolders.getSrc().substring(start, placeHolder.getStart()));
      sb.append(placeHolder.getField().getValue(tuple));
      start = placeHolder.getEnd();
    }
    sb.append(placeHolders.getSrc().substring(start));

    URI uri = new URI(sb.toString());
    List<NameValuePair> parameters = URLEncodedUtils.parse(uri.getQuery(), Charsets.UTF_8);
    return new URIBuilder(uri).setParameters(parameters).build();
  }

  private void readValues(JsonNode node, int layer, List<List<Object>> valuesList)
      throws IOException {
    if (node.isObject()) {
      if (rootPath != null && layer < rootPath.size()) {
        JsonNode childNode = node.get((String) rootPath.get(layer));
        if (childNode != null) {
          readValues(childNode, ++layer, valuesList);
        }
      } else {
        List<Object> values = Lists.newArrayList();
        Map<String, Object> valuesMap = mapper.readValue(node.traverse(), mapper.getTypeFactory()
            .constructMapType(LinkedHashMap.class, String.class, Object.class));
        for (String field : fetchFieldNames) {
          values.add(valuesMap.get(field));
        }
        valuesList.add(values);
      }
    } else if (node.isArray()) {
      if (rootPath != null && layer < rootPath.size()) {
        if (rootPath.get(layer) instanceof Integer) {
          JsonNode childNode = node.get((Integer) rootPath.get(layer));
          if (childNode != null) {
            readValues(childNode, ++layer, valuesList);
          }
        } else {
          for (int i = 0; i < node.size(); i++) {
            readValues(node.get(i), layer, valuesList);
          }
        }
      } else {
        for (int i = 0; i < node.size(); i++) {
          readValues(node.get(i), layer, valuesList);
        }
      }
    }
  }

  private List<List<Object>> readValues(String content) throws IOException {
    List<List<Object>> valuesList = Lists.newArrayList();
    readValues(mapper.readTree(content), 0, valuesList);
    return valuesList;
  }

  private List<List<Object>> execute(URI requestUri) throws IOException, ProcessorException {
    HttpGet request = new HttpGet(requestUri);
    CloseableHttpResponse response = null;
    try {
      response = client.execute(request);
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Execute get request to '{}'", requestUri);
        }
      } else {
        throw new ProcessorException("Failed to execute get repuest to '" + requestUri
            + "' status:" + response.getStatusLine().getStatusCode());
      }
      String content = EntityUtils.toString(response.getEntity());
      return readValues(content);
    } finally {
      if (response != null) {
        try {
          response.close();
        } catch (IOException e) {
          LOG.error("Failed to close response");
        }
      }
    }
  }

  @Override
  public List<List<Object>> fetch(GungnirTuple tuple) throws ProcessorException {
    if (client == null) {
      throw new ProcessorException("Processor isn't open");
    }

    List<List<Object>> valuesList = null;
    try {
      final URI requestUri = getRequestUri(placeHolders, tuple);

      if (expireSecs > 0) {
        valuesList = cache.get(requestUri.toString(), new Callable<List<List<Object>>>() {

          @Override
          public List<List<Object>> call() throws Exception {
            return execute(requestUri);
          }
        });
      } else {
        valuesList = execute(requestUri);
      }

      return valuesList;
    } catch (URISyntaxException e) {
      throw new ProcessorException("Failed to execute request to '" + url + "'", e);
    } catch (ExecutionException e) {
      throw new ProcessorException("Failed to execute request to '" + url + "'", e);
    } catch (IOException e) {
      throw new ProcessorException("Failed to execute request to '" + url + "'", e);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.error("Failed to close client");
      }
    }

    LOG.info("WebFetchProcessor closed({})", this);
  }

  @Override
  public String toString() {
    return "web_fetch(" + url + ", " + rootPathString + ", " + Arrays.toString(fetchFieldNames)
        + ")";
  }
}
