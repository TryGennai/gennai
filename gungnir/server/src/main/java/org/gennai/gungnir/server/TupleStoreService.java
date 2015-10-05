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

package org.gennai.gungnir.server;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.server.tuplestore.RewriteRules;
import org.gennai.gungnir.tuple.persistent.TrackingData;
import org.gennai.gungnir.utils.GungnirUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultCookie;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.MediaType;
import com.twitter.util.Future;
import com.twitter.util.Promise;

public class TupleStoreService extends Service<HttpRequest, HttpResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(TupleStoreService.class);

  private static final Pattern POST_URI_PATTERN = Pattern.compile("^/(\\w+)/(\\w+)/json/?$");
  private static final int REST_URI_LENGTH = GUNGNIR_REST_URI.length();
  private Executor executor;
  private GungnirManager manager;
  private Integer trackingMaxage;
  private RewriteRules rewriteRules;
  private Meter requestCount;

  public TupleStoreService() {
    executor = Executors.newCachedThreadPool(GungnirUtils.createThreadFactory("TupleStoreService"));
    manager = GungnirManager.getManager();
    GungnirConfig config = manager.getConfig();
    trackingMaxage = config.getInteger(TRACKING_COOKIE_MAXAGE);
    rewriteRules = new RewriteRules(config);
    requestCount = manager.getMetricsManager().getRegistry().meter(METRICS_REQUEST_COUNT);
  }

  private HttpResponse track(HttpRequest request, String accountId, String tupleName) {
    requestCount.mark();

    String content = request.getContent().toString(CharsetUtil.UTF_8);

    Cookie tidCookie = null;
    String cookieString = request.headers().get(COOKIE);
    if (cookieString != null) {
      CookieDecoder cookieDecoder = new CookieDecoder();
      Set<Cookie> cookies = cookieDecoder.decode(cookieString);
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(TID_COOKIE_NAME)) {
          tidCookie = cookie;
        }
      }
    }

    HttpResponse response = new DefaultHttpResponse(request.getProtocolVersion(), NO_CONTENT);
    HttpHeaders.setHeader(response, CONTENT_LENGTH, 0);
    HttpHeaders.setDateHeader(response, DATE, new Date());
    try {
      if (tidCookie == null || tidCookie.getValue() == null) {
        TrackingData trackingData = new TrackingData(tupleName, content);
        manager.dispatchTrackingData(accountId, trackingData);

        if (trackingData.getTid() != null) {
          DefaultCookie cookie = new DefaultCookie(TID_COOKIE_NAME, trackingData.getTid());
          cookie.setMaxAge(trackingMaxage);
          CookieEncoder cookieEncoder = new CookieEncoder(true);
          cookieEncoder.addCookie(cookie);
          HttpHeaders.setHeader(response, SET_COOKIE, cookieEncoder.encode());
        }
      } else {
        LOG.debug("Tracking ID '{}' in cookie", tidCookie.getValue());

        TrackingData trackingData = new TrackingData(tupleName, content, tidCookie.getValue());
        manager.dispatchTrackingData(accountId, trackingData);
      }
    } catch (MetaStoreException e) {
      LOG.error(e.getMessage(), e);
    } catch (NotStoredException e) {
      LOG.error(e.getMessage(), e);
    }

    return response;
  }

  private HttpResponse version(HttpRequest request) {
    HttpResponse response = new DefaultHttpResponse(request.getProtocolVersion(), OK);
    response.setContent(ChannelBuffers.copiedBuffer(
        "{version: \"" + GUNGNIR_VERSION_STRING + "\"}",
        CharsetUtil.UTF_8));
    HttpHeaders.setHeader(response, CONTENT_LENGTH, response.getContent().readableBytes());
    HttpHeaders.setDateHeader(response, DATE, new Date());
    return response;
  }

  @Override
  public Future<HttpResponse> apply(final HttpRequest request) {
    final Promise<HttpResponse> promise = new Promise<HttpResponse>();

    executor.execute(new Runnable() {

      @Override
      public void run() {
        String uri = rewriteRules.rewrite(request.getUri());

        if (uri != null && uri.startsWith(GUNGNIR_REST_URI)) {
          if (uri.length() > REST_URI_LENGTH) {
            uri = uri.substring(REST_URI_LENGTH);
            Matcher matcher = POST_URI_PATTERN.matcher(uri);
            if (matcher.find() && request.getMethod() == POST
                && request.headers().get(CONTENT_TYPE).equals(MediaType.Json())) {
              promise.setValue(track(request, matcher.group(1), matcher.group(2)));
            } else if (uri.isEmpty() || "/".equals(uri)) {
              promise.setValue(version(request));
            } else {
              promise.setValue(new DefaultHttpResponse(request.getProtocolVersion(), NOT_FOUND));
            }
          } else {
            promise.setValue(version(request));
          }
        } else {
          promise.setValue(new DefaultHttpResponse(request.getProtocolVersion(), NOT_FOUND));
        }
      }
    });

    return promise;
  }
}
