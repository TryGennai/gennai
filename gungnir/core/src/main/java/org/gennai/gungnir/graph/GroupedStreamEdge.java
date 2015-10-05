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

package org.gennai.gungnir.graph;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.tuple.TupleAccessor;

public class GroupedStreamEdge extends StreamEdge {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private GroupFields groupFields;

  public GroupedStreamEdge(GroupFields groupFields) {
    super();
    this.groupFields = groupFields;
  }

  public GroupedStreamEdge(GroupFields groupFields, TupleAccessor[] selector) {
    super(selector);
    this.groupFields = groupFields;
  }

  private GroupedStreamEdge(GroupedStreamEdge c) {
    super(c);
    this.groupFields = c.groupFields;
  }

  public GroupFields getGroupFields() {
    return groupFields;
  }

  @Override
  public String toString() {
    if (getSelector() == null) {
      return "GS[" + groupFields + "]";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("GS[");
      sb.append(groupFields);
      sb.append("](");
      for (int i = 0; i < getSelector().length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getSelector()[i]);
      }
      sb.append(')');
      return sb.toString();
    }
  }

  @Override
  public GroupedStreamEdge clone() {
    return new GroupedStreamEdge(this);
  }
}
