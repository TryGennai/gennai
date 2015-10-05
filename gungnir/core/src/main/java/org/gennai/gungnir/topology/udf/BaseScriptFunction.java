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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Arrays;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.FunctionEntity.ScriptType;
import org.gennai.gungnir.utils.GungnirUtils;

public abstract class BaseScriptFunction extends BaseFunction<Object> implements UserDefined {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private FunctionEntity function;
  private ScriptType scriptType;
  private transient String classPath;
  private transient ScriptEngine engine;

  protected BaseScriptFunction(FunctionEntity function, ScriptType scriptType, String classPath) {
    this.function = function;
    this.scriptType = scriptType;
    this.classPath = classPath;
  }

  protected BaseScriptFunction(BaseScriptFunction c) {
    super(c);
    this.function = c.function;
    this.scriptType = c.scriptType;
    this.classPath = c.classPath;
  }

  @Override
  public FunctionEntity getFunction() {
    return function;
  }

  protected Invocable getEngine() throws IOException, ScriptException {
    if (classPath == null) {
      classPath = getConfig().getString(CLASS_PATH);
    }

    if (engine == null) {
      engine = new ScriptEngineManager().getEngineByName(scriptType.getShortName());

      ClassLoader classLoader = null;
      if (classPath != null) {
        classLoader = GungnirUtils.addToClassPath(Paths.get(classPath));
      }
      if (classLoader == null) {
        classLoader = Thread.currentThread().getContextClassLoader();
      }

      InputStreamReader reader =
          new InputStreamReader(classLoader.getResourceAsStream(function.getLocation()));
      try {
        engine.eval(reader);
      } finally {
        reader.close();
      }
    }

    return (Invocable) engine;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(function.getName());

    try {
      sb.append('(');
      if (getParameters() != null) {
        for (int i = 0; i < getParameters().length; i++) {
          if (i > 0) {
            sb.append(", ");
          }
          if (getParameters()[i].getClass().isArray()) {
            sb.append(Arrays.toString((Object[]) getParameters()[i]));
          } else {
            sb.append(getParameters()[i].toString());
          }
        }
      }
      sb.append(')');
      if (getAliasName() != null) {
        sb.append(" AS ");
        sb.append(getAliasName());
      }
    } catch (Exception e) {
      sb.append(e);
    }

    return sb.toString();
  }
}
