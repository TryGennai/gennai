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

package org.gennai.gungnir.server.resources;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.tuple.persistent.TrackingData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

@Path("/gungnir/v{version}")
public class GungnirResource {

  private static final Logger LOG = LoggerFactory.getLogger(GungnirResource.class);

  private GungnirManager manager;
  private Integer trackingMaxage;
  private Meter requestCount;

  public GungnirResource() {
    manager = GungnirManager.getManager();
    GungnirConfig config = manager.getConfig();
    trackingMaxage = config.getInteger(TRACKING_COOKIE_MAXAGE);
    requestCount = manager.getMetricsManager().getRegistry().meter(METRICS_REQUEST_COUNT);

    LOG.debug("create resource {}", this);
  }

  private static boolean validateProtocolVersion(String version) {
    String[] ver = version.split("\\.");
    if (ver.length == 2) {
      if (new Byte(ver[0]).equals(GUNGNIR_PROTOCOL_VERSION[0])
          && new Byte(ver[1]).equals(GUNGNIR_PROTOCOL_VERSION[1])) {
        return true;
      }
    }
    return false;
  }

  @POST
  @Path("/{accountId}/{tupleName}/json")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response track(@Context UriInfo uriInfo, @PathParam("version") String version,
      @PathParam("accountId") String accountId, @PathParam("tupleName") String tupleName,
      @CookieParam(TID_COOKIE_NAME) Cookie tidCookie, String content) {
    if (!validateProtocolVersion(version)) {
      LOG.warn("Unknown protocol version '{}'", version);
      return Response.status(Response.Status.NO_CONTENT).build();
    }

    requestCount.mark();

    try {
      if (tidCookie == null || tidCookie.getValue() == null) {
        TrackingData trackingData = new TrackingData(tupleName, content);
        manager.dispatchTrackingData(accountId, trackingData);
        if (trackingData.getTid() != null) {
          return Response.status(Response.Status.NO_CONTENT).cookie(new NewCookie(TID_COOKIE_NAME,
              trackingData.getTid(), null, null, null, trackingMaxage, false)).build();
        } else {
          return Response.status(Response.Status.NO_CONTENT).build();
        }
      } else {
        LOG.debug("Tracking ID '{}' in cookie", tidCookie.getValue());

        TrackingData trackingData = new TrackingData(tupleName, content, tidCookie.getValue());
        manager.dispatchTrackingData(accountId, trackingData);
        return Response.status(Response.Status.NO_CONTENT).build();
      }
    } catch (MetaStoreException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(Response.Status.NO_CONTENT).build();
    } catch (NotStoredException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(Response.Status.NO_CONTENT).build();
    }
  }

  @GET
  @Path("/")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.status(Response.Status.OK)
        .entity("{version: \"" + GUNGNIR_VERSION_STRING + "\"}").build();
  }
}
