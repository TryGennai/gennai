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

import java.util.List;

import com.google.common.collect.Lists;

public class ComplexJoinKey implements JoinKey {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<SimpleJoinKey> joinKeys;

  public ComplexJoinKey() {
    joinKeys = Lists.newArrayList();
  }

  @Override
  public String getTupleName() {
    if (!joinKeys.isEmpty()) {
      return joinKeys.get(0).getTupleName();
    }
    return null;
  }

  public List<SimpleJoinKey> getJoinKeys() {
    return joinKeys;
  }

  public void add(SimpleJoinKey joinKey) {
    joinKeys.add(joinKey);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((joinKeys == null) ? 0 : joinKeys.hashCode());
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
    ComplexJoinKey other = (ComplexJoinKey) obj;
    if (joinKeys == null) {
      if (other.joinKeys != null) {
        return false;
      }
    } else if (!joinKeys.equals(other.joinKeys)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return joinKeys.toString();
  }
}
