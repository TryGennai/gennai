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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Maps;

@BaseFunction.Description(name = "regexp_extract")
public class RegexpExtract extends BaseFunction<String> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Map<String, Pattern> patternsCache;

  public RegexpExtract() {
  }

  private RegexpExtract(RegexpExtract c) {
    super(c);
  }

  @Override
  public RegexpExtract create(Object... parameters) throws ArgumentException {
    if (parameters.length == 3) {
      setParameters(parameters);
    } else {
      throw new ArgumentException("Incorrect number of arguments");
    }
    return this;
  }

  @Override
  protected void prepare() {
    patternsCache = Maps.newHashMap();
  }

  @Override
  public String evaluate(GungnirTuple tuple) {
    String subject = null;
    if (getParameter(0) instanceof Field) {
      Object value = ((Field) getParameter(0)).getValue(tuple);
      if (value != null) {
        subject = value.toString();
      }
    } else {
      subject = getParameter(0).toString();
    }

    String pattern = null;
    if (getParameter(1) instanceof Field) {
      Object value = ((Field) getParameter(1)).getValue(tuple);
      if (value != null) {
        pattern = value.toString();
      }
    } else {
      pattern = getParameter(1).toString();
    }

    Integer index = null;
    if (getParameter(2) instanceof Field) {
      Object value = ((Field) getParameter(2)).getValue(tuple);
      if (value != null && value instanceof Integer) {
        index = (Integer) value;
      }
    } else {
      if (getParameter(2) != null && getParameter(2) instanceof Integer) {
        index = (Integer) getParameter(2);
      }
    }

    if (subject != null && pattern != null) {
      Pattern p = patternsCache.get(pattern);
      if (p == null) {
        p = Pattern.compile(pattern);
        patternsCache.put(pattern, p);
      }
      Matcher matcher = p.matcher(subject);
      if (matcher.find()) {
        if (index != null) {
          if (matcher.groupCount() >= index) {
            return matcher.group(index);
          }
        } else {
          return matcher.group();
        }
      }
    }
    return null;
  }

  @Override
  public RegexpExtract clone() {
    return new RegexpExtract(this);
  }
}
