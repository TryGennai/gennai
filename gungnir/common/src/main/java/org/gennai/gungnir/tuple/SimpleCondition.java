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

package org.gennai.gungnir.tuple;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

public class SimpleCondition implements Condition {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public enum Type {
    EQ("="),
    NE("<>"),
    GT(">"),
    GE(">="),
    LT("<"),
    LE("<="),
    LIKE(),
    REGEXP(),
    IN(),
    ALL(),
    BETWEEN(),
    IS_NULL("IS NULL"),
    IS_NOT_NULL("IS NOT NULL");

    private String displayString;

    private Type(String displayString) {
      this.displayString = displayString;
    }

    private Type() {
      this.displayString = this.name();
    }

    public String getDisplayString() {
      return displayString;
    }
  }

  private Type type;
  private Field field;
  private Object value;

  public SimpleCondition(Type type, Field field, Object value) {
    this.type = type;
    this.field = field;
    this.value = value;
  }

  private SimpleCondition(SimpleCondition c) {
    this.type = c.type;
    this.field = c.field;
    this.value = c.value;
  }

  public Type getType() {
    return type;
  }

  public Field getField() {
    return field;
  }

  public void setField(Field field) {
    this.field = field;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public List<String> getFieldNames() {
    return Lists.newArrayList(field.getFieldName());
  }

  @Override
  public SimpleCondition clone() {
    return new SimpleCondition(this);
  }

  @Override
  public String toString() {
    if (value == null) {
      return field + " " + type.getDisplayString();
    } else {
      if (value.getClass().isArray()) {
        return field + " " + type.getDisplayString() + " " + Arrays.toString((Object[]) value);
      } else {
        return field + " " + type.getDisplayString() + " " + value;
      }
    }
  }
}
