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

import java.util.List;

public class TupleValues implements Cloneable {

  private String tupleName;
  private List<Object> values;

  TupleValues() {
  }

  public TupleValues(String tupleName, List<Object> values) {
    this.tupleName = tupleName;
    this.values = values;
  }

  private TupleValues(TupleValues c) {
    this.tupleName = c.tupleName;
    this.values = c.values;
  }

  public void setTupleName(String tupleName) {
    this.tupleName = tupleName;
  }

  public String getTupleName() {
    return tupleName;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

  public List<Object> getValues() {
    return values;
  }

  @Override
  public TupleValues clone() {
    return new TupleValues(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tupleName == null) ? 0 : tupleName.hashCode());
    result = prime * result + ((values == null) ? 0 : values.hashCode());
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
    TupleValues other = (TupleValues) obj;
    if (tupleName == null) {
      if (other.tupleName != null) {
        return false;
      }
    } else if (!tupleName.equals(other.tupleName)) {
      return false;
    }
    if (values == null) {
      if (other.values != null) {
        return false;
      }
    } else if (!values.equals(other.values)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return tupleName + " " + values;
  }
}
