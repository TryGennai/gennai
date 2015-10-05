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

package org.gennai.gungnir.topology.udf;

import static org.gennai.gungnir.GungnirConst.*;

import java.net.MalformedURLException;
import java.net.URL;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;

@BaseFunction.Description(name = "parse_url")
public class ParseUrl extends BaseFunction<String> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private enum Part {
    HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, USERINFO
  }

  public ParseUrl() {
  }

  private ParseUrl(ParseUrl c) {
    super(c);
  }

  @Override
  public ParseUrl create(Object... parameters) throws ArgumentException {
    if (parameters.length == 2 || parameters.length == 3) {
      setParameters(parameters);
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public String evaluate(GungnirTuple tuple) {
    String urlString = null;
    if (getParameter(0) instanceof Field) {
      Object value = ((Field) getParameter(0)).getValue(tuple);
      if (value != null) {
        urlString = value.toString();
      }
    } else {
      urlString = getParameter(0).toString();
    }

    Part partToExtract = null;
    if (getParameter(1) instanceof Field) {
      Object value = ((Field) getParameter(1)).getValue(tuple);
      if (value != null) {
        try {
          partToExtract = Part.valueOf(value.toString());
        } catch (IllegalArgumentException e) {
          return urlString;
        }
      }
    } else {
      try {
        partToExtract = Part.valueOf(getParameter(1).toString());
      } catch (IllegalArgumentException e) {
        return urlString;
      }
    }

    String keyToExtract = null;
    if (numParameters() == 3) {
      if (getParameter(2) instanceof Field) {
        Object value = ((Field) getParameter(2)).getValue(tuple);
        if (value != null) {
          keyToExtract = value.toString();
        }
      } else {
        keyToExtract = getParameter(2).toString();
      }
    }

    if (urlString != null && partToExtract != null) {
      URL url;
      try {
        url = new URL(urlString);
      } catch (MalformedURLException e) {
        return null;
      }
      switch (partToExtract) {
        case HOST:
          return url.getHost();
        case PATH:
          return url.getPath();
        case QUERY:
          if (keyToExtract != null) {
            String query = url.getQuery();
            if (query != null) {
              String[] params = query.split("&");
              for (String param : params) {
                String[] p = param.split("=");
                if (p[0].equals(keyToExtract) && p.length == 2) {
                  return p[1];
                }
              }
            }
          }
          return null;
        case REF:
          return url.getRef();
        case PROTOCOL:
          return url.getProtocol();
        case AUTHORITY:
          return url.getAuthority();
        case FILE:
          return url.getFile();
        case USERINFO:
          return url.getUserInfo();
        default:
          return null;
      }
    }
    return null;
  }

  @Override
  public ParseUrl clone() {
    return new ParseUrl(this);
  }
}
