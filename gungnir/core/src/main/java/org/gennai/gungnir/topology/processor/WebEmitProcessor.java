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
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.json.StructSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class WebEmitProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(WebEmitProcessor.class);

  private enum Format {
    NONE, ES
  }

  private String url;
  private Format format;
  private Map<String, String> param;
  private transient Map<String, List<String>> outputFieldNames;
  private transient CloseableHttpClient client;
  private transient ObjectMapper mapper;
  private transient String actionLine;

  public WebEmitProcessor(String url, String format, Map<String, String> param) {
    this.url = url;
    // TODO check param before submit topology
    try {
      this.format = Format.valueOf(format.toUpperCase());
    } catch (IllegalArgumentException e) {
      this.format = Format.NONE;
    }
    this.param = param;
  }

  public WebEmitProcessor(String url) {
    this(url, Format.NONE.name(), null);
  }

  public WebEmitProcessor(String url, String format) {
    this(url, format, null);
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    url = context.replaceVariable(url);
    this.outputFieldNames = outputFieldNames;

    client = HttpClientBuilder.create().build();

    SimpleModule module = new SimpleModule("GungnirModule",
        new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null, null, null));
    module.addSerializer(Struct.class, new StructSerializer());

    mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    mapper.setDateFormat(sdf);
    mapper.registerModule(module);
    mapper.configure(Feature.ESCAPE_NON_ASCII, true);

    if (format == Format.ES && param != null && param.containsKey("index")
        && param.containsKey("type")) {
      ObjectNode metadataNode = mapper.createObjectNode();
      metadataNode.put("_index", param.get("index"));
      metadataNode.put("_type", param.get("type"));

      ObjectNode actionNode = mapper.createObjectNode();
      actionNode.set("index", metadataNode);

      try {
        actionLine = mapper.writeValueAsString(actionNode) + '\n';
      } catch (JsonGenerationException e) {
        throw new ProcessorException("Failed to convert json format", e);
      } catch (JsonMappingException e) {
        throw new ProcessorException("Failed to convert json format", e);
      } catch (IOException e) {
        throw new ProcessorException("Failed to convert json format", e);
      }
    }

    LOG.info("WebEmitProcessor opened({})", this);
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    if (client == null) {
      throw new ProcessorException("Processor isn't open");
    }

    List<Map<String, Object>> records = Lists.newArrayListWithCapacity(tuples.size());
    for (TupleValues tupleValues : tuples) {
      List<String> fieldNames = outputFieldNames.get(tupleValues.getTupleName());
      if (fieldNames.size() > 0) {
        Map<String, Object> record = Maps.newLinkedHashMap();
        for (int i = 0; i < fieldNames.size(); i++) {
          record.put(fieldNames.get(i), tupleValues.getValues().get(i));
        }
        records.add(record);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Emit to '{}' {}", url, record);
        }
      }
    }

    if (records.size() > 0) {
      // TODO No2
      HttpPost request = new HttpPost(url);
      try {
        if (actionLine != null) {
          StringBuilder sb = new StringBuilder();
          for (Map<String, Object> record : records) {
            sb.append(actionLine);
            sb.append(mapper.writeValueAsString(record));
            sb.append('\n');
          }
          request.setEntity(new StringEntity(sb.toString()));
        } else {
          request.addHeader("Content-Type", "application/json");
          request.setEntity(new StringEntity(mapper.writeValueAsString(records)));
        }
      } catch (JsonGenerationException e) {
        throw new ProcessorException("Failed to convert json format", e);
      } catch (JsonMappingException e) {
        throw new ProcessorException("Failed to convert json format", e);
      } catch (IOException e) {
        throw new ProcessorException("Failed to convert json format", e);
      }

      CloseableHttpResponse response = null;
      try {
        response = client.execute(request);
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Execute post request to '{}' {}", url, records);
          }
        } else {
          throw new ProcessorException("Failed to execute post request to '" + url + "' status:"
              + response.getStatusLine().getStatusCode());
        }
        EntityUtils.consume(response.getEntity());
      } catch (ClientProtocolException e) {
        throw new ProcessorException("Failed to execute post request to '" + url + "'", e);
      } catch (IOException e) {
        throw new ProcessorException("Failed to execute post request to '" + url + "'", e);
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

    LOG.info("WebEmitProcessor closed({})", this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("web_emit(");
    sb.append(url);
    switch (format) {
      case ES:
        sb.append(", Elasticsearch, ");
        sb.append(param);
        break;
      default:
    }
    sb.append(')');
    return sb.toString();
  }
}
