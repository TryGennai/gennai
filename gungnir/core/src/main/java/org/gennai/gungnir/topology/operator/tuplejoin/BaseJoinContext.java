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

public abstract class BaseJoinContext implements JoinContext {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  protected static FieldAccessor getJoinField(FieldAccessor field) {
    if (field.getParentAccessor() != null) {
      FieldAccessor parent = getJoinField(field.getParentAccessor());
      if (field.getOriginalName().equals(field.getFieldName())) {
        return new FieldAccessor(field.getOriginalName(), parent);
      } else {
        return new FieldAccessor(field.getOriginalName(), parent).as(field.getFieldName());
      }
    } else {
      String fieldName = null;
      if (field.getTupleAccessor() != null) {
        fieldName = "+" + field.getTupleAccessor().getTupleName() + ":" + field.getOriginalName();
      } else {
        fieldName = field.getOriginalName();
      }
      if (field.getOriginalName().equals(field.getFieldName())) {
        return new FieldAccessor(fieldName);
      } else {
        return new FieldAccessor(fieldName).as(field.getFieldName());
      }
    }
  }
}
