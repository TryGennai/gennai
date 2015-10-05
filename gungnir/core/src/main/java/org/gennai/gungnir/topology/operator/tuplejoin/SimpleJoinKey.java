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

package org.gennai.gungnir.topology.operator.tuplejoin;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.tuple.FieldAccessor;


public class SimpleJoinKey implements JoinKey {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private FieldAccessor keyField;

  public SimpleJoinKey(FieldAccessor keyField) {
    this.keyField = keyField;
  }

  @Override
  public String getTupleName() {
    FieldAccessor field = keyField;
    while (field.getParentAccessor() != null) {
      field = field.getParentAccessor();
    }
    return field.getTupleAccessor().getTupleName();
  }

  public FieldAccessor getKeyField() {
    return keyField;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((keyField == null) ? 0 : keyField.hashCode());
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
    SimpleJoinKey other = (SimpleJoinKey) obj;
    if (keyField == null) {
      if (other.keyField != null) {
        return false;
      }
    } else if (!keyField.equals(other.keyField)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return keyField.toString();
  }
}
