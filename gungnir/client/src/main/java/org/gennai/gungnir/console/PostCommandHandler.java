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

package org.gennai.gungnir.console;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.history.FileHistory;

import org.apache.commons.lang.StringUtils;
import org.gennai.gungnir.client.GungnirClient.Statement;
import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.Console.CommandHandler;
import org.gennai.gungnir.thrift.GungnirServerException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.finagle.ChannelWriteException;
import com.twitter.finagle.FailedFastException;
import com.twitter.finagle.Http;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.MediaType;
import com.twitter.finagle.util.DefaultTimer;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.TimeoutException;

public class PostCommandHandler implements CommandHandler {

  private static final Pattern POST_COMMAND_PATTERN = Pattern
      .compile("^POST\\s+(\\w+)(?:\\s+(\\{.+\\})|)$", Pattern.CASE_INSENSITIVE);
  private static final int POST_RESPONSE_TIMEOUT = 10000;

  private Map<String, Cookie> cookiesMap;
  private ConsoleContext context;
  private ObjectMapper mapper;

  PostCommandHandler(Map<String, Cookie> cookiesMap) {
    this.cookiesMap = cookiesMap;
  }

  @Override
  public void prepare(ConsoleContext context) {
    this.context = context;
    this.mapper = new ObjectMapper();
  }

  @Override
  public boolean isMatch(String command) {
    return command.toUpperCase().startsWith("POST");
  }

  @SuppressWarnings("unchecked")
  private Object readValue(String fieldName, Map<String, Object> fieldType, String indent)
      throws IOException {
    String typeName = (String) fieldType.get("type");

    if ("TIMESTAMP".equals(typeName)) {
      String dateFormat = (String) fieldType.get("dateFormat");
      if (dateFormat != null) {
        typeName += "(" + dateFormat + ")";
      }
      context.getReader().setPrompt(indent + fieldName + " (" + typeName + "): ");
      for (;;) {
        String value = context.getReader().readLine();
        context.getReader().getHistory().removeLast();

        if (!value.isEmpty()) {
          try {
            if (dateFormat != null) {
              SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
              sdf.parse(value);
              return value;
            } else {
              long longValue = Long.parseLong(value);
              long time = TimeUnit.MILLISECONDS.convert(longValue, TimeUnit.SECONDS);
              new Date(time);
              return longValue;
            }
          } catch (Exception e) {
            context.getReader().println("FAILED: " + e.getMessage());
          }
        }
      }
    } else if ("LIST".equals(typeName)) {
      context.getReader().println(indent + fieldName + " (" + typeName + ")");
      Map<String, Object> elementType = (Map<String, Object>) fieldType.get("element");
      List<Object> list = Lists.newArrayList();
      for (;;) {
        Object value = readValue(String.valueOf(list.size()), elementType, indent + "  ");
        if (value == null) {
          break;
        }
        list.add(value);
      }
      return list;
    } else if ("MAP".equals(typeName)) {
      context.getReader().println(indent + fieldName + " (" + typeName + ")");
      Map<String, Object> keyType = (Map<String, Object>) fieldType.get("key");
      Map<String, Object> valueType = (Map<String, Object>) fieldType.get("value");
      Map<Object, Object> map = Maps.newLinkedHashMap();
      for (;;) {
        Object key = readValue("key", keyType, indent + "  ");
        if (key == null) {
          break;
        }
        Object value = readValue("value", valueType, indent + "  ");
        map.put(key, value);
      }
      return map;
    } else if ("STRUCT".equals(typeName)) {
      context.getReader().println(indent + fieldName + " (" + typeName + ")");
      Map<String, Map<String, Object>> fields = (Map<String, Map<String, Object>>) fieldType
          .get("fields");
      Map<Object, Object> struct = Maps.newLinkedHashMap();
      for (Map.Entry<String, Map<String, Object>> entry : fields.entrySet()) {
        Object value = readValue(entry.getKey(), entry.getValue(), indent + "  ");
        if (value != null) {
          struct.put(entry.getKey(), value);
        }
      }
      return struct;
    } else {
      context.getReader().setPrompt(indent + fieldName + " (" + typeName + "): ");
      String value = null;
      while ((value = context.getReader().readLine()) != null) {
        context.getReader().getHistory().removeLast();

        try {
          if (value.isEmpty()) {
            return null;
          }
          if ("STRING".equals(typeName)) {
            return value;
          } else if ("TINYINT".equals(typeName)) {
            return Byte.parseByte(value);
          } else if ("SMALLINT".equals(typeName)) {
            return Short.parseShort(value);
          } else if ("INT".equals(typeName)) {
            return Integer.parseInt(value);
          } else if ("BIGINT".equals(typeName)) {
            return Long.parseLong(value);
          } else if ("FLOAT".equals(typeName)) {
            return Float.parseFloat(value);
          } else if ("DOUBLE".equals(typeName)) {
            return Double.parseDouble(value);
          } else if ("BOOLEAN".equals(typeName)) {
            return Boolean.parseBoolean(value);
          } else {
            return value;
          }
        } catch (Exception e) {
          context.getReader().println("FAILED: " + e.getMessage());
        }
      }
    }
    return null;
  }

  private String readJsonTuple(Statement statement, String tupleName, String desc)
      throws IOException {
    Map<String, Object> descTuple = mapper.readValue(desc, mapper.getTypeFactory()
        .constructMapType(LinkedHashMap.class, String.class, Object.class));
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> fields = (Map<String, Map<String, Object>>) descTuple
        .get("fields");

    Map<String, Object> fieldsMap = Maps.newLinkedHashMap();
    for (Map.Entry<String, Map<String, Object>> entry : fields.entrySet()) {
      if (!(entry.getKey().equals(TRACKING_ID_FIELD))
          && !(entry.getKey().equals(TRACKING_NO_FIELD))
          && !(entry.getKey().equals(ACCEPT_TIME_FIELD))) {
        fieldsMap.put(entry.getKey(), readValue(entry.getKey(), entry.getValue(), ""));
      }
    }

    return mapper.writeValueAsString(fieldsMap);
  }

  private void parseCookie(List<String> cookieStrings) {
    if (cookieStrings != null) {
      List<Cookie> cookies = Lists.newArrayList();
      CookieDecoder decoder = new CookieDecoder();
      for (String cookieString : cookieStrings) {
        cookies.addAll(decoder.decode(cookieString));
      }
      for (Cookie cookie : cookies) {
        cookiesMap.put(cookie.getName(), cookie);
      }
    }
  }

  private void sendTuple(String accountId, String tupleName, String jsonTuple) throws IOException {
    String uri = GUNGNIR_REST_URI + "/" + accountId + "/" + tupleName + "/json";
    String dest = null;
    if (context.getConfig().getString(CLUSTER_MODE).equals(LOCAL_CLUSTER)) {
      dest = context.getConfig().getString(TUPLE_STORE_SERVER_HOST) + ':'
          + context.getConfig().getInteger(TUPLE_STORE_SERVER_PORT);
    } else {
      List<String> zkServers = context.getConfig().getList(CLUSTER_ZOOKEEPER_SERVERS);
      dest = "zk!" + StringUtils.join(zkServers, ",") + "!"
          + context.getConfig().getString(GUNGNIR_NODE_PATH) + STORES_NODE_PATH;
    }

    context.getReader().println("POST " + uri);

    Service<HttpRequest, HttpResponse> client = Http.newService(dest);

    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    ChannelBuffer content = ChannelBuffers.copiedBuffer(jsonTuple, CharsetUtil.UTF_8);
    request.headers().set(CONTENT_TYPE, MediaType.Json());
    request.headers().set(CONTENT_LENGTH, content.readableBytes());
    request.setContent(content);

    if (cookiesMap.size() > 0) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, Cookie> entry : cookiesMap.entrySet()) {
        sb.append(entry.getKey());
        sb.append('=');
        sb.append(entry.getValue().getValue());
        sb.append(';');
      }
      request.headers().set(COOKIE, sb.toString());
    }

    String msg = null;
    try {
      HttpResponse response =
          Await.result(client.apply(request).raiseWithin(
              new Duration(TimeUnit.MILLISECONDS.toNanos(POST_RESPONSE_TIMEOUT)),
              DefaultTimer.twitter()));
      parseCookie(response.headers().getAll(SET_COOKIE));
      msg = "OK";
    } catch (TimeoutException e) {
      msg = "FAILED: Timed out";
    } catch (ChannelWriteException e) {
      msg = "FAILED: Channel has been disconnected";
    } catch (FailedFastException e) {
      msg = "FAILED: Channel has been disconnected";
    } catch (NoBrokersAvailableException e) {
      msg = "FAILED: There is no available brokers";
    } catch (Exception e) {
      msg = "FAILED: " + e.getMessage();
    }

    context.getReader().println(msg);

    client.close();
  }

  @Override
  public void execute(String command) {
    try {
      try {
        Matcher matcher = POST_COMMAND_PATTERN.matcher(command);
        if (matcher.find()) {
          String tupleName = matcher.group(1);
          String jsonTuple = matcher.group(2);

          String desc = context.getStatement().execute("DESC TUPLE " + tupleName);

          if (jsonTuple == null) {
            jsonTuple = readJsonTuple(context.getStatement(), tupleName, desc);
            command = command + " " + jsonTuple;
            context.getReader().println(command);
          }
          sendTuple(context.getAccountId(), tupleName, jsonTuple);
        } else {
          context.getReader().println("POST commnad usage: POST TUPLENAME [JSON_TUPLE]");
        }

        context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
        ((FileHistory) context.getReader().getHistory()).flush();
      } catch (GungnirServerException e) {
        context.getReader().println("FAILED: " + e.getMessage());
      } catch (GungnirClientException e) {
        context.getReader().println("FAILED: " + e.getMessage());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
