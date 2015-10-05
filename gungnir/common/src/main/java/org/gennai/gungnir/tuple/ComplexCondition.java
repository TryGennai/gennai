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

import java.util.List;

import com.google.common.collect.Lists;

public class ComplexCondition implements Condition {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public enum Type {
    AND, OR, NOT
  }

  private Type type;
  private List<Condition> conditions;

  public ComplexCondition(Type type, Condition... condition) {
    this.type = type;
    this.conditions = Lists.newArrayList(condition);
  }

  private ComplexCondition(ComplexCondition c) {
    this.type = c.type;
    this.conditions = Lists.newArrayList();
    for (Condition cond : c.conditions) {
      conditions.add(cond.clone());
    }
  }

  public Type getType() {
    return type;
  }

  public List<Condition> getConditions() {
    return conditions;
  }

  void addCondition(Condition condition) {
    conditions.add(condition);
  }

  @Override
  public List<String> getFieldNames() {
    List<String> fieldNames = Lists.newArrayList();
    for (Condition condition : conditions) {
      fieldNames.addAll(condition.getFieldNames());
    }
    return fieldNames;
  }

  @Override
  public ComplexCondition clone() {
    return new ComplexCondition(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Condition condition : conditions) {
      if (sb.length() > 1) {
        sb.append(" " + type + " ");
      }
      if (type == Type.NOT) {
        sb.append(type + " ");
      }
      if (condition instanceof ComplexCondition) {
        sb.append('(');
      }
      sb.append(condition);
      if (condition instanceof ComplexCondition) {
        sb.append(')');
      }
    }
    return sb.toString();
  }
}
