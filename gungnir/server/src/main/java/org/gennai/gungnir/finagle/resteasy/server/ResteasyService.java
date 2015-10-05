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

package org.gennai.gungnir.finagle.resteasy.server;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.ws.rs.ext.RuntimeDelegate.HeaderDelegate;

import org.gennai.gungnir.utils.GungnirUtils;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.plugins.server.netty.NettyUtil;
import org.jboss.resteasy.specimpl.ResteasyHttpHeaders;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.jboss.resteasy.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.finagle.Service;
import com.twitter.util.Future;
import com.twitter.util.Promise;

public final class ResteasyService extends Service<HttpRequest, HttpResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(ResteasyService.class);

  private final SynchronousDispatcher dispatcher;
  private final Executor executor;

  private ResteasyService(SynchronousDispatcher dispatcher, Executor executor) {
    this.dispatcher = dispatcher;
    this.executor = executor;
  }

  public interface ResourceDeclarer {

    OptionalDeclarer resource(Object... resources);
  }

  public interface BuildDeclarer {

    ResteasyService build();
  }

  public interface OptionalDeclarer extends BuildDeclarer {

    BuildDeclarer executor(Executor executor);
  }

  public static final class Builder implements ResourceDeclarer, OptionalDeclarer {

    private final SynchronousDispatcher dispatcher;
    private Executor executor;

    private Builder() {
      dispatcher = new SynchronousDispatcher(ResteasyProviderFactory.getInstance());
    }

    @Override
    public OptionalDeclarer resource(Object... resources) {
      for (Object resource : resources) {
        dispatcher.getRegistry().addSingletonResource(resource);
      }
      return this;
    }

    @Override
    public Builder executor(Executor executor) {
      this.executor = executor;
      return this;
    }

    @Override
    public ResteasyService build() {
      if (executor == null) {
        executor = Executors.newSingleThreadExecutor(
            GungnirUtils.createThreadFactory("RestServerService"));
      }
      return new ResteasyService(dispatcher, executor);
    }
  }

  public static ResourceDeclarer builder() {
    return new ResteasyService.Builder();
  }

  @Override
  public Future<HttpResponse> apply(HttpRequest request) {
    Promise<HttpResponse> promise = new Promise<HttpResponse>();
    this.executor.execute(new Invoker(request, dispatcher, promise));
    return promise;
  }

  private static class Invoker implements Runnable {

    private final HttpRequest request;
    private final SynchronousDispatcher dispatcher;
    private final Promise<HttpResponse> promise;

    public Invoker(HttpRequest request, SynchronousDispatcher dispatcher,
        Promise<HttpResponse> promise) {
      this.request = request;
      this.dispatcher = dispatcher;
      this.promise = promise;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void run() {
      ResteasyHttpHeaders headers = NettyUtil.extractHttpHeaders(request);
      ResteasyUriInfo uriInfo = extractUriInfo(request);

      ResteasyHttpResponse resteasyResponse = new ResteasyHttpResponse();
      ResteasyHttpRequest resteasyRequest = new ResteasyHttpRequest(headers, uriInfo,
          request.getMethod().getName(), dispatcher, resteasyResponse);
      ChannelBufferInputStream is = new ChannelBufferInputStream(request.getContent());
      resteasyRequest.setInputStream(is);

      dispatcher.invoke(resteasyRequest, resteasyResponse);

      HttpResponseStatus status = HttpResponseStatus.valueOf(resteasyResponse.getStatus());
      HttpResponse response = new DefaultHttpResponse(request.getProtocolVersion(), status);
      for (Map.Entry<String, List<Object>> entry : resteasyResponse.getOutputHeaders().entrySet()) {
        String key = entry.getKey();
        for (Object value : entry.getValue()) {
          HeaderDelegate delegate = dispatcher.getProviderFactory().createHeaderDelegate(
              value.getClass());
          if (delegate != null) {
            response.headers().set(key, delegate.toString(value));
          } else {
            response.headers().set(key, value.toString());
          }
        }
      }

      try {
        response.setContent(resteasyResponse.getBuffer());
      } catch (IOException e) {
        LOG.error("Failed to get response buffer", e);
        response = new DefaultHttpResponse(request.getProtocolVersion(), INTERNAL_SERVER_ERROR);
      }

      response.headers().set(CONTENT_LENGTH, response.getContent().readableBytes());
      response.headers().set(DATE, DateUtil.formatDate(new Date()));

      promise.setValue(response);
    }
  }

  private static ResteasyUriInfo extractUriInfo(HttpRequest request) {
    String protocol = request.getProtocolVersion().getProtocolName().toLowerCase();
    String host = HttpHeaders.getHost(request, "unknown");
    String uri = request.getUri();
    URI absoluteURI = URI.create(protocol + "://" + host + uri);
    ResteasyUriInfo uriInfo = new ResteasyUriInfo(absoluteURI);
    return uriInfo;
  }
}
