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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Lists;

public class GroupFields implements Serializable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private FieldAccessor[] fields;
  private GroupFields parent;

  public GroupFields(FieldAccessor[] fields) {
    this.fields = fields;
  }

  public GroupFields(FieldAccessor[] fields, GroupFields parent) {
    this.fields = fields;
    this.parent = parent;
  }

  public FieldAccessor[] getFields() {
    return fields;
  }

  public GroupFields getParent() {
    return parent;
  }

  private void getValues(GungnirTuple tuple, List<Object> values) {
    if (parent != null) {
      parent.getValues(tuple, values);
    }
    for (FieldAccessor field : fields) {
      values.add(field.getValue(tuple));
    }
  }

  public List<Object> getValues(GungnirTuple tuple) {
    List<Object> values = Lists.newArrayList();
    getValues(tuple, values);
    return values;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(fields);
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    GroupFields other = (GroupFields) obj;
    if (!Arrays.equals(fields, other.fields)) {
      return false;
    }
    if (parent == null) {
      if (other.parent != null) {
        return false;
      }
    } else if (!parent.equals(other.parent)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (parent != null) {
      sb.append(parent.toString());
      sb.append(", ");
    }
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fields[i]);
    }
    return sb.toString();
  }
}
