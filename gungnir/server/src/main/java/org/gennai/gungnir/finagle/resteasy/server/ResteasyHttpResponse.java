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
import java.io.OutputStream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NewCookie;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

public class ResteasyHttpResponse implements org.jboss.resteasy.spi.HttpResponse {

  private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
  private MultivaluedMap<String, Object> outputHeaders;
  private ChannelBufferOutputStream underlyingOutputStream;
  private OutputStream os;

  public ResteasyHttpResponse() {
    outputHeaders = new MultivaluedMapImpl<String, Object>();
    underlyingOutputStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer());
    os = underlyingOutputStream;
  }

  public HttpResponseStatus getResponseStatus() {
    return responseStatus;
  }

  @Override
  public void setOutputStream(OutputStream os) {
    this.os = os;
  }

  public ChannelBuffer getBuffer() throws IOException {
    return underlyingOutputStream.buffer();
  }

  @Override
  public int getStatus() {
    return responseStatus.getCode();
  }

  @Override
  public void setStatus(int status) {
    this.responseStatus = HttpResponseStatus.valueOf(status);
  }

  @Override
  public MultivaluedMap<String, Object> getOutputHeaders() {
    return outputHeaders;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return os;
  }

  @Override
  public void addNewCookie(NewCookie cookie) {
    outputHeaders.add(HttpHeaders.SET_COOKIE, cookie);
  }

  @Override
  public void sendError(int status) throws IOException {
    this.responseStatus = HttpResponseStatus.valueOf(status);
  }

  @Override
  public void sendError(int status, String message) throws IOException {
    message = message.replaceAll("\r", "").replaceAll("\n", "");
    this.responseStatus = new HttpResponseStatus(status, message);
  }

  @Override
  public boolean isCommitted() {
    return false;
  }

  @Override
  public void reset() {
    outputHeaders.clear();
    underlyingOutputStream.buffer().clear();
    outputHeaders.clear();
  }
}
