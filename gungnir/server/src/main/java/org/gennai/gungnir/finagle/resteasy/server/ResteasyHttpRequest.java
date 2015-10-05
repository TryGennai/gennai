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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.core.SynchronousExecutionContext;
import org.jboss.resteasy.plugins.providers.FormUrlEncodedProvider;
import org.jboss.resteasy.specimpl.ResteasyHttpHeaders;
import org.jboss.resteasy.spi.ResteasyAsynchronousContext;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.jboss.resteasy.util.Encode;

import com.google.common.collect.Maps;

public class ResteasyHttpRequest implements org.jboss.resteasy.spi.HttpRequest {

  private ResteasyHttpHeaders httpHeaders;
  private SynchronousDispatcher dispatcher;
  private ResteasyUriInfo uriInfo;
  private String httpMethod;
  private MultivaluedMap<String, String> formParameters;
  private MultivaluedMap<String, String> decodedFormParameters;
  private InputStream inputStream;
  private Map<String, Object> attributes = Maps.newHashMap();
  private ResteasyHttpResponse httpResponse;

  public ResteasyHttpRequest(ResteasyHttpHeaders httpHeaders, ResteasyUriInfo uri,
      String httpMethod, SynchronousDispatcher dispatcher, ResteasyHttpResponse httpResponse) {
    this.httpResponse = httpResponse;
    this.dispatcher = dispatcher;
    this.httpHeaders = httpHeaders;
    this.httpMethod = httpMethod;
    this.uriInfo = uri;
  }

  @Override
  public MultivaluedMap<String, String> getMutableHeaders() {
    return httpHeaders.getMutableHeaders();
  }

  @Override
  public void setHttpMethod(String method) {
    this.httpMethod = method;
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    Enumeration<String> en = new Enumeration<String>()
    {
      private Iterator<String> it = attributes.keySet().iterator();

      @Override
      public boolean hasMoreElements() {
        return it.hasNext();
      }

      @Override
      public String nextElement() {
        return it.next();
      }
    };
    return en;
  }

  @Override
  public ResteasyAsynchronousContext getAsyncContext() {
    return new SynchronousExecutionContext(dispatcher, this, httpResponse);
  }

  @Override
  public MultivaluedMap<String, String> getFormParameters() {
    if (formParameters != null) {
      return formParameters;
    }

    if (getHttpHeaders().getMediaType().isCompatible(
        MediaType.valueOf("application/x-www-form-urlencoded"))) {
      try {
        formParameters = FormUrlEncodedProvider.parseForm(getInputStream());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IllegalArgumentException(
          "Request media type isn't application/x-www-form-urlencoded");
    }

    return formParameters;
  }

  @Override
  public MultivaluedMap<String, String> getDecodedFormParameters() {
    if (decodedFormParameters != null) {
      return decodedFormParameters;
    }
    decodedFormParameters = Encode.decode(getFormParameters());
    return decodedFormParameters;
  }

  @Override
  public Object getAttribute(String attribute) {
    return attributes.get(attribute);
  }

  @Override
  public void setAttribute(String name, Object value) {
    attributes.put(name, value);
  }

  @Override
  public void removeAttribute(String name) {
    attributes.remove(name);
  }

  @Override
  public HttpHeaders getHttpHeaders() {
    return httpHeaders;
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public void setInputStream(InputStream stream) {
    this.inputStream = stream;
  }

  @Override
  public ResteasyUriInfo getUri() {
    return uriInfo;
  }

  @Override
  public String getHttpMethod() {
    return httpMethod;
  }

  @Override
  public void setRequestUri(URI requestUri) {
    uriInfo = uriInfo.setRequestUri(requestUri);
  }

  @Override
  public void setRequestUri(URI baseUri, URI requestUri) {
    uriInfo = new ResteasyUriInfo(baseUri.resolve(requestUri));
  }

  @Override
  public boolean isInitial() {
    return true;
  }

  @Override
  public void forward(String path) {
  }

  @Override
  public boolean wasForwarded() {
    return false;
  }
}
