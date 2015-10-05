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

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.tuple.Struct;

public interface FieldType extends Serializable {

  public enum TypeDef {
    STRING(String.class), TINYINT(Byte.class, byte.class), SMALLINT(Short.class, short.class),
    INT(Integer.class, int.class), BIGINT(Long.class, long.class), FLOAT(Float.class, float.class),
    DOUBLE(Double.class, double.class), BOOLEAN(Boolean.class, boolean.class),
    TIMESTAMP(Date.class), LIST(List.class), MAP(Map.class), STRUCT(Struct.class);

    private Type javaType;
    private Type primitiveType;

    TypeDef(Type javaType) {
      this.javaType = javaType;
    }

    TypeDef(Type javaType, Type primitiveType) {
      this.javaType = javaType;
      this.primitiveType = primitiveType;
    }

    public Type getJavaType() {
      return javaType;
    }

    public Type getPrimitiveType() {
      return primitiveType;
    }
  }

  String getName();

  Type getJavaType();

  boolean isInstance(Object obj);

  @Override
  int hashCode();

  @Override
  boolean equals(Object obj);
}
