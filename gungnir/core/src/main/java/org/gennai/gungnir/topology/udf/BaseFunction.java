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

package org.gennai.gungnir.topology.udf;

import static org.gennai.gungnir.GungnirConst.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.ArithNode;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.InternalArithNode;
import org.gennai.gungnir.tuple.BaseField;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class BaseFunction<T> extends BaseField implements Function<T> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Description {

    String name();
  }

  private Object[] parameters;
  private String aliasName;
  private transient GungnirConfig config;
  private transient GungnirContext context;
  private transient String description;

  protected BaseFunction() {
  }

  protected BaseFunction(BaseFunction<T> c) {
    if (c.parameters != null) {
      parameters = new Object[c.parameters.length];
      for (int i = 0; i < c.parameters.length; i++) {
        if (c.parameters[i] instanceof Function<?>) {
          parameters[i] = ((Function<?>) c.parameters[i]).clone();
        } else {
          parameters[i] = c.parameters[i];
        }
      }
    }
    this.aliasName = c.aliasName;
  }

  protected void setParameters(Object[] parameters) {
    this.parameters = parameters;
  }

  protected Object[] getParameters() {
    return parameters;
  }

  protected Object getParameter(int index) {
    if (parameters != null && index >= 0 && index < parameters.length) {
      return parameters[index];
    }
    return null;
  }

  protected int numParameters() {
    if (parameters != null) {
      return parameters.length;
    }
    return 0;
  }

  protected boolean hasParameter() {
    return parameters != null;
  }

  protected void setAliasName(String aliasName) {
    this.aliasName = aliasName;
  }

  protected String getAliasName() {
    return aliasName;
  }

  @Override
  public String getFieldName() {
    return aliasName;
  }

  public GungnirConfig getConfig() {
    return config;
  }

  public GungnirContext getContext() {
    return context;
  }

  @Override
  public Function<T> as(String aliasName) {
    this.aliasName = aliasName;
    return this;
  }

  @Override
  public Object getValue(GungnirTuple tuple) {
    return evaluate(tuple);
  }

  @Override
  public List<FieldAccessor> getFields() {
    if (parameters != null) {
      Set<FieldAccessor> fields = Sets.newLinkedHashSet();
      for (Object parameter : parameters) {
        if (parameter instanceof FieldAccessor) {
          fields.add((FieldAccessor) parameter);
        } else if (parameter instanceof Function<?>) {
          List<FieldAccessor> funcFields = ((Function<?>) parameter).getFields();
          if (funcFields != null) {
            fields.addAll(funcFields);
          }
        } else if (parameter instanceof ArithNode) {
          fields.addAll(((ArithNode) parameter).getFields());
        }
      }
      return Lists.newArrayList(fields);
    }
    return null;
  }

  protected abstract void prepare();

  public void prepare(GungnirConfig config, GungnirContext context) {
    this.config = config;
    this.context = context;

    if (parameters != null) {
      for (Object parameter : parameters) {
        if (parameter instanceof Function<?>) {
          ((Function<?>) parameter).prepare(config, context);
        } else if (parameter instanceof InternalArithNode) {
          ((InternalArithNode) parameter).prepare(config, context);
        }
      }
    }

    prepare();
  }

  @Override
  public abstract Function<T> clone();

  @Override
  public String toString() {
    if (description == null) {
      Description desc = this.getClass().getAnnotation(Description.class);
      if (desc == null) {
        return super.toString();
      }

      StringBuilder sb = new StringBuilder();
      sb.append(desc.name());

      try {
        sb.append('(');
        if (parameters != null) {
          for (int i = 0; i < parameters.length; i++) {
            if (i > 0) {
              sb.append(", ");
            }
            if (parameters[i].getClass().isArray()) {
              sb.append(Arrays.toString((Object[]) parameters[i]));
            } else {
              sb.append(parameters[i].toString());
            }
          }
        }
        sb.append(')');
        if (aliasName != null) {
          sb.append(" AS ");
          sb.append(aliasName);
        }
      } catch (Exception e) {
        sb.append(e);
      }

      description = sb.toString();
    }
    return description;
  }
}
