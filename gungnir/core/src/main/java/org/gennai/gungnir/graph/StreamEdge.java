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

import java.io.Serializable;

import org.gennai.gungnir.tuple.TupleAccessor;

public class StreamEdge implements Serializable, Cloneable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private TupleAccessor[] selector;

  public StreamEdge() {
  }

  public StreamEdge(TupleAccessor[] selector) {
    this.selector = selector;
  }

  protected StreamEdge(StreamEdge c) {
    this.selector = c.selector;
  }

  public void setSelector(TupleAccessor[] selector) {
    this.selector = selector;
  }

  public TupleAccessor[] getSelector() {
    return selector;
  }

  @Override
  public String toString() {
    if (selector == null) {
      return "S";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("S(");
      for (int i = 0; i < selector.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(selector[i]);
      }
      sb.append(')');
      return sb.toString();
    }
  }

  @Override
  public StreamEdge clone() {
    return new StreamEdge(this);
  }
}
