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

public final class PrimitiveType implements FieldType {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public static final PrimitiveType STRING = new PrimitiveType(TypeDef.STRING);
  public static final PrimitiveType TINYINT = new PrimitiveType(TypeDef.TINYINT);
  public static final PrimitiveType SMALLINT = new PrimitiveType(TypeDef.SMALLINT);
  public static final PrimitiveType INT = new PrimitiveType(TypeDef.INT);
  public static final PrimitiveType BIGINT = new PrimitiveType(TypeDef.BIGINT);
  public static final PrimitiveType FLOAT = new PrimitiveType(TypeDef.FLOAT);
  public static final PrimitiveType DOUBLE = new PrimitiveType(TypeDef.DOUBLE);
  public static final PrimitiveType BOOLEAN = new PrimitiveType(TypeDef.BOOLEAN);

  private TypeDef typeDef;

  private PrimitiveType(TypeDef typeName) {
    this.typeDef = typeName;
  }

  @Override
  public String getName() {
    return typeDef.name();
  }

  @Override
  public Type getJavaType() {
    return typeDef.getJavaType();
  }

  @Override
  public boolean isInstance(Object obj) {
    if (obj.getClass() != typeDef.getJavaType()) {
      return false;
    }
    return true;
  }

  public static PrimitiveType valueOf(String typeName) {
    return new PrimitiveType(TypeDef.valueOf(typeName));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((typeDef == null) ? 0 : typeDef.hashCode());
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
    PrimitiveType other = (PrimitiveType) obj;
    if (typeDef != other.typeDef) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return typeDef.toString();
  }
}
