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

import javax.script.Invocable;
import javax.script.ScriptException;

import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.FunctionEntity.ScriptType;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScriptAggregateFunction extends BaseScriptFunction
    implements AggregateFunction<Object> {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(ScriptAggregateFunction.class);

  private Invocable engine;

  public ScriptAggregateFunction(FunctionEntity function, ScriptType scriptType, String classPath) {
    super(function, scriptType, classPath);
  }

  private ScriptAggregateFunction(ScriptAggregateFunction c) {
    super(c);
  }

  @Override
  public Function<Object> create(Object... parameters) throws SemanticAnalyzeException,
      ArgumentException {
    setParameters(parameters);
    return this;
  }

  @Override
  protected void prepare() {
    try {
      engine = getEngine();
    } catch (IOException e) {
      LOG.error("Failed to read script file", e);
    } catch (ScriptException e) {
      LOG.error("Failed to evaluate script file", e);
    }
  }

  @Override
  public Object evaluate(GungnirTuple tuple) {
    Object[] args = new Object[getParameters().length];
    for (int i = 0; i < getParameters().length; i++) {
      if (getParameter(i) instanceof Field) {
        args[i] = ((Field) getParameter(i)).getValue(tuple);
      } else {
        args[i] = getParameter(i);
      }
    }

    try {
      return engine.invokeFunction("evaluate", args);
    } catch (Exception e) {
      LOG.warn("Failed to invoke script", e);
    }

    return null;
  }


  @Override
  public Object exclude(GungnirTuple tuple) {
    Object[] args = new Object[getParameters().length];
    for (int i = 0; i < getParameters().length; i++) {
      if (getParameter(i) instanceof AggregateFunction<?>) {
        args[i] = ((AggregateFunction<?>) getParameter(i)).exclude(tuple);
      } else if (getParameter(i) instanceof Field) {
        args[i] = ((Field) getParameter(i)).getValue(tuple);
      } else {
        args[i] = getParameter(i);
      }
    }

    try {
      return engine.invokeFunction("exclude", args);
    } catch (Exception e) {
      LOG.warn("Failed to invoke script", e);
    }

    return null;
  }

  @Override
  public void clear() {
    for (int i = 0; i < getParameters().length; i++) {
      if (getParameter(i) instanceof AggregateFunction<?>) {
        ((AggregateFunction<?>) getParameter(i)).clear();
      }
    }

    try {
      engine.invokeFunction("clear");
    } catch (Exception e) {
      LOG.warn("Failed to invoke script", e);
    }
  }

  @Override
  public ScriptAggregateFunction clone() {
    return new ScriptAggregateFunction(this);
  }
}
