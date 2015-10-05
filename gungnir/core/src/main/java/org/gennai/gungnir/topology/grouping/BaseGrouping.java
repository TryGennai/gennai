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

package org.gennai.gungnir.topology.grouping;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.topology.GungnirContext;

public abstract class BaseGrouping implements Grouping {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String partitionName;
  private GungnirContext context;

  @Override
  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  protected String getPartitionName() {
    return partitionName;
  }

  @Override
  public void setContext(GungnirContext context) {
    this.context = context;
  }

  protected GungnirContext getContext() {
    return context;
  }

  protected static int hashIndex(Object x, int n) {
    int h = x.hashCode();
    h += ~(h << 9);
    h ^= (h >>> 14);
    h += (h << 4);
    h ^= (h >>> 10);
    return Math.abs(h) % n;
  }
}
