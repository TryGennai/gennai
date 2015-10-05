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
import java.util.List;

public class ListType implements FieldType {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private FieldType elementType;

  public FieldType getElementType() {
    return elementType;
  }

  public ListType(FieldType elementType) {
    this.elementType = elementType;
  }

  @Override
  public String getName() {
    return TypeDef.LIST.name();
  }

  @Override
  public Type getJavaType() {
    return TypeDef.LIST.getJavaType();
  }

  @Override
  public boolean isInstance(Object obj) {
    if (!(obj instanceof List)) {
      return false;
    }
    List<?> list = (List<?>) obj;
    for (Object element : list) {
      if (!elementType.isInstance(element)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((elementType == null) ? 0 : elementType.hashCode());
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
    ListType other = (ListType) obj;
    if (elementType == null) {
      if (other.elementType != null) {
        return false;
      }
    } else if (!elementType.equals(other.elementType)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getName() + "<" + elementType + ">";
  }
}
