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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.tuple.FieldAccessor;

import com.google.common.collect.Lists;

public final class ProcessorUtils {

  private static final Pattern ESCAPE_PATTERN = Pattern.compile("\\\\@");
  private static final Pattern FIELD_PATTERN = Pattern.compile("@(\\w+(\\.\\w+)*)!?");

  private ProcessorUtils() {
  }

  public static final class PlaceHolder implements Serializable {

    private static final long serialVersionUID = SERIAL_VERSION_UID;

    private FieldAccessor field;
    private int start;
    private int end;

    private PlaceHolder(FieldAccessor field, int start, int end) {
      this.field = field;
      this.start = start;
      this.end = end;
    }

    public FieldAccessor getField() {
      return field;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }
  }

  public static final class PlaceHolders implements Serializable, Iterable<PlaceHolder> {

    private static final long serialVersionUID = SERIAL_VERSION_UID;

    private String src;
    private List<PlaceHolder> placeHolders = Lists.newArrayList();

    private PlaceHolders(String src) {
      this.src = src;
    }

    public String getSrc() {
      return src;
    }

    private void add(FieldAccessor field, int start, int end) {
      placeHolders.add(new PlaceHolder(field, start, end));
    }

    @Override
    public Iterator<PlaceHolder> iterator() {
      return placeHolders.iterator();
    }

    public boolean isEmpty() {
      if (placeHolders == null) {
        return true;
      }
      return placeHolders.isEmpty();
    }

    public int size() {
      return placeHolders.size();
    }

    @Override
    public String toString() {
      return src;
    }
  }

  public static PlaceHolders findPlaceHolders(String src) {
    StringBuilder sb = new StringBuilder();
    int start = 0;
    List<Integer> escapes = Lists.newArrayList();
    Matcher matcher = ESCAPE_PATTERN.matcher(src);
    while (matcher.find()) {
      escapes.add(matcher.start() - escapes.size());
      sb.append(src.substring(start, matcher.start()));
      sb.append('@');
      start = matcher.end();
    }
    sb.append(src.substring(start));
    src = sb.toString();

    PlaceHolders placeHolders = new PlaceHolders(src);
    matcher = FIELD_PATTERN.matcher(src);
    while (matcher.find()) {
      if (!escapes.contains(matcher.start())) {
        String fieldName = matcher.group(1);
        String[] names = fieldName.split("\\.");
        FieldAccessor field = null;
        for (String name : names) {
          if (field == null) {
            field = new FieldAccessor(name);
          } else {
            field = new FieldAccessor(name, field);
          }
        }
        placeHolders.add(field, matcher.start(), matcher.end());
      }
    }
    return placeHolders;
  }
}
