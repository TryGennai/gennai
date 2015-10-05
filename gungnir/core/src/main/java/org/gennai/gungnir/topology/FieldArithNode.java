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

package org.gennai.gungnir.topology;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Lists;

public class FieldArithNode implements ExternalArithNode {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Field value;

  public FieldArithNode(Field value) {
    this.value = value;
  }

  public void prepare(GungnirConfig config, GungnirContext context) {
    if (value instanceof Function<?>) {
      ((Function<?>) value).prepare(config, context);
    }
  }

  @Override
  public Number getValue(GungnirTuple tuple) {
    Object v = value.getValue(tuple);
    if (v instanceof Number) {
      return (Number) v;
    }
    return 0;
  }

  @Override
  public List<FieldAccessor> getFields() {
    if (value instanceof FieldAccessor) {
      return Lists.newArrayList((FieldAccessor) value);
    } else if (value instanceof Function<?>) {
      return ((Function<?>) value).getFields();
    }
    return null;
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
