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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvokeFunction extends BaseInvokeFunction {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(InvokeFunction.class);

  private transient List<InvokableMethod> evaluateMethods;
  private transient Object instance;

  public InvokeFunction(FunctionEntity function, String classPath) {
    super(function, classPath);
  }

  private InvokeFunction(InvokeFunction c) {
    super(c);
  }

  @Override
  public InvokeFunction create(Object... parameters) throws SemanticAnalyzeException,
      ArgumentException {
    setParameters(parameters);

    try {
      evaluateMethods = getMethods(getFunctionClass(), "evaluate");
    } catch (IOException e) {
      throw new SemanticAnalyzeException("Failed to create class loader " + getClassPath(), e);
    } catch (ClassNotFoundException e) {
      throw new SemanticAnalyzeException("'" + getFunction().getLocation() + "' isn't registered",
          e);
    }
    if (evaluateMethods.isEmpty()) {
      throw new SemanticAnalyzeException("evaluate method is undefined");
    }

    return this;
  }

  @Override
  protected void prepare() {
    try {
      evaluateMethods = getMethods(getFunctionClass(), "evaluate");
      instance = getFunctionClass().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create instance {}", getFunction().getLocation(), e);
    }
  }

  @Override
  public Object evaluate(GungnirTuple tuple) {
    if (!evaluateMethods.isEmpty() && instance != null) {
      Object[] args = new Object[getParameters().length];
      for (int i = 0; i < getParameters().length; i++) {
        if (getParameter(i) instanceof Field) {
          args[i] = ((Field) getParameter(i)).getValue(tuple);
        } else {
          args[i] = getParameter(i);
        }
      }

      try {
        List<InvokableMethod> invokeMethods = findInvokeMethods(evaluateMethods, args);
        if (invokeMethods == null) {
          LOG.warn("Can't found {} method", evaluateMethods.get(0).getName());
        } else if (invokeMethods.size() == 1) {
          return invokeMethods.get(0).invoke(instance, args);
        } else {
          LOG.warn("Ambiguous method call {}", evaluateMethods.get(0).getName());
        }
      } catch (IllegalAccessException e) {
        LOG.warn("Failed to invoke method", e);
      } catch (IllegalArgumentException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to invoke method", e);
        }
      } catch (InvocationTargetException e) {
        LOG.warn("Failed to invoke method", e);
      }
    }

    return null;
  }

  @Override
  public InvokeFunction clone() {
    return new InvokeFunction(this);
  }
}
