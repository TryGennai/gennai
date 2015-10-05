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

package org.gennai.gungnir.tuple.schema;

import static org.gennai.gungnir.GungnirConst.*;

import java.lang.reflect.Type;
import java.util.Map;

public class MapType implements FieldType {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private FieldType keyType;
  private FieldType valueType;

  public MapType(FieldType keyType, FieldType valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  public FieldType getKeyType() {
    return keyType;
  }

  public FieldType getValueType() {
    return valueType;
  }

  @Override
  public String getName() {
    return TypeDef.MAP.name();
  }

  @Override
  public Type getJavaType() {
    return TypeDef.MAP.getJavaType();
  }

  @Override
  public boolean isInstance(Object obj) {
    if (!(obj instanceof Map)) {
      return false;
    }
    Map<?, ?> map = (Map<?, ?>) obj;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!keyType.isInstance(entry.getKey())) {
        return false;
      }
      if (!valueType.isInstance(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((keyType == null) ? 0 : keyType.hashCode());
    result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
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
    MapType other = (MapType) obj;
    if (keyType == null) {
      if (other.keyType != null) {
        return false;
      }
    } else if (!keyType.equals(other.keyType)) {
      return false;
    }
    if (valueType == null) {
      if (other.valueType != null) {
        return false;
      }
    } else if (!valueType.equals(other.valueType)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getName() + "<" + keyType + "," + valueType + ">";
  }
}
