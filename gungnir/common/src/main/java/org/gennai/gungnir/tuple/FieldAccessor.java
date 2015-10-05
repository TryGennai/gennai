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
import java.util.Map;

import com.google.common.collect.Lists;

public class FieldAccessor extends BaseField {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String fieldName;
  private List<Object> subscripts;
  private TupleAccessor tupleAccessor;
  private FieldAccessor parentAccessor;
  private String aliasFieldName;

  public FieldAccessor(String fieldName) {
    this.fieldName = fieldName;
  }

  public FieldAccessor(String fieldName, TupleAccessor tupleAccessor) {
    this.fieldName = fieldName;
    this.tupleAccessor = tupleAccessor;
  }

  public FieldAccessor(String fieldName, FieldAccessor parentAccessor) {
    this.fieldName = fieldName;
    this.parentAccessor = parentAccessor;
  }

  public String getOriginalName() {
    return fieldName;
  }

  @Override
  public String getFieldName() {
    if (aliasFieldName != null) {
      return aliasFieldName;
    } else {
      return fieldName;
    }
  }

  public List<Object> getSubscripts() {
    return subscripts;
  }

  public TupleAccessor getTupleAccessor() {
    return tupleAccessor;
  }

  public FieldAccessor getParentAccessor() {
    return parentAccessor;
  }

  public FieldAccessor field(String name) {
    return new FieldAccessor(name, this);
  }

  public boolean isWildcardField() {
    return "*".equals(getFieldName());
  }

  public boolean isContextField() {
    if (parentAccessor != null && parentAccessor.getFieldName().equals(CONTEXT_FIELD)) {
      return true;
    }
    return false;
  }

  public FieldAccessor select(Object subscript) {
    if (subscripts == null) {
      subscripts = Lists.newArrayList();
    }
    subscripts.add(subscript);
    return this;
  }

  public FieldAccessor as(String aliasFieldName) {
    this.aliasFieldName = aliasFieldName;
    return this;
  }

  @SuppressWarnings("unchecked")
  private Object selectValue(Object value, GungnirTuple tuple) {
    if (subscripts != null) {
      for (Object subscript : subscripts) {
        if (value instanceof Map) {
          if (subscript instanceof Field) {
            value = ((Map<Object, Object>) value).get(((Field) subscript).getValue(tuple));
          } else {
            value = ((Map<Object, Object>) value).get(subscript);
          }
          if (value == null) {
            return null;
          }
        } else if (value instanceof List) {
          int index = 0;
          if (subscript instanceof Field) {
            Object s = ((Field) subscript).getValue(tuple);
            if (s instanceof Number) {
              index = ((Number) s).intValue();
            } else {
              return null;
            }
          } else {
            try {
              index = Integer.parseInt(subscript.toString());
            } catch (NumberFormatException e) {
              return null;
            }
          }

          if (index < 0 || index >= ((List<Object>) value).size()) {
            return null;
          }
          value = ((List<Object>) value).get(index);
        } else {
          return null;
        }
      }
    }
    return value;
  }

  @Override
  public Object getValue(GungnirTuple tuple) {
    if (parentAccessor != null) {
      Object value = parentAccessor.getValue(tuple);
      if (value != null) {
        value = ((Struct) value).getValueByField(fieldName);
        return selectValue(value, tuple);
      } else {
        return null;
      }
    } else {
      if (tupleAccessor != null) {
        if (tuple.getTupleName().equals(tupleAccessor.getTupleName())) {
          Object value = tuple.getValueByField(fieldName);
          if (value == null) {
            return null;
          }
          return selectValue(value, tuple);
        } else {
          return null;
        }
      } else {
        Object value = tuple.getValueByField(fieldName);
        if (value != null) {
          return selectValue(value, tuple);
        } else {
          return null;
        }
      }
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
    result = prime * result + ((parentAccessor == null) ? 0 : parentAccessor.hashCode());
    result = prime * result + ((subscripts == null) ? 0 : subscripts.hashCode());
    result = prime * result + ((tupleAccessor == null) ? 0 : tupleAccessor.hashCode());
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
    FieldAccessor other = (FieldAccessor) obj;
    if (fieldName == null) {
      if (other.fieldName != null) {
        return false;
      }
    } else if (!fieldName.equals(other.fieldName)) {
      return false;
    }
    if (parentAccessor == null) {
      if (other.parentAccessor != null) {
        return false;
      }
    } else if (!parentAccessor.equals(other.parentAccessor)) {
      return false;
    }
    if (subscripts == null) {
      if (other.subscripts != null) {
        return false;
      }
    } else if (!subscripts.equals(other.subscripts)) {
      return false;
    }
    if (tupleAccessor == null) {
      if (other.tupleAccessor != null) {
        return false;
      }
    } else if (!tupleAccessor.equals(other.tupleAccessor)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (tupleAccessor != null) {
      sb.append(tupleAccessor);
      sb.append(':');
    }
    if (parentAccessor != null) {
      sb.append(parentAccessor);
      sb.append('.');
    }
    sb.append(fieldName);
    if (subscripts != null) {
      for (Object subScript : subscripts) {
        sb.append("[" + subScript + "]");
      }
    }
    if (aliasFieldName != null) {
      sb.append(" AS " + aliasFieldName);
    }
    return sb.toString();
  }
}
