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

package org.gennai.gungnir;

import static org.gennai.gungnir.GungnirConfig.*;

import java.io.IOException;
import java.util.EnumSet;

import org.gennai.gungnir.metastore.InMemoryMetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.FunctionEntity.FunctionType;
import org.gennai.gungnir.ql.FunctionEntity.ScriptType;
import org.gennai.gungnir.ql.FunctionValidateException;
import org.gennai.gungnir.ql.analysis.FileRegistry;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.topology.udf.ScriptAggregateFunction;
import org.gennai.gungnir.topology.udf.ScriptFunction;
import org.gennai.gungnir.tuple.GungnirTuple;

public final class ScriptFunctionInvoker {

  private static final String USER_NAME = "TEST_USER";
  private static final String ACCOUNT_ID = "TEST_ACCOUNT";
  private static final String SESSION_ID = "TEST_SESSION_ID";
  private static final String STATEMENT_ID = "TEST_STATEMENT_ID";

  private GungnirConfig config;
  private UserEntity owner;
  private FileRegistry fileRegistry;
  private FunctionEntity function;
  private Function<Object> invokeFunction;

  private ScriptFunctionInvoker(String fileName) throws Exception {
    config = GungnirManager.getManager().getConfig();
    config.put(GungnirConfig.METASTORE, InMemoryMetaStore.class.getName());
    owner = new UserEntity(USER_NAME);
    owner.setId(ACCOUNT_ID);
    fileRegistry = new FileRegistry(new StatementEntity(STATEMENT_ID, SESSION_ID, owner));

    function = new FunctionEntity();
    function.setName("func");
    function.setLocation(fileName);
  }

  private void validate() throws FunctionValidateException {
    function.setOwner(owner);

    try {
      function.validate(fileRegistry);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (MetaStoreException e) {
      throw new RuntimeException(e);
    }
  }

  private void create(Object... parameters) throws Exception {
    validate();

    ScriptType scriptType = null;
    EnumSet<ScriptType> scriptTypes = EnumSet.allOf(ScriptType.class);
    for (ScriptType script : scriptTypes) {
      if (function.getLocation().endsWith(script.getExtension())) {
        scriptType = script;
        break;
      }
    }

    if (function.getType() == FunctionType.UDF) {
      invokeFunction =
          new ScriptFunction(function, scriptType, config.getString(LOCAL_DIR)).create(parameters);
      invokeFunction.prepare(config, new GungnirContext());
    } else {
      invokeFunction =
          new ScriptAggregateFunction(function, scriptType, config.getString(LOCAL_DIR))
              .create(parameters);
      invokeFunction.prepare(config, new GungnirContext());
    }
  }

  public static void validate(String fileName) throws Exception {
    ScriptFunctionInvoker invoker = new ScriptFunctionInvoker(fileName);
    invoker.validate();
  }

  public static ScriptFunctionInvoker create(String fileName, Object... parameters)
      throws Exception {
    ScriptFunctionInvoker invoker = new ScriptFunctionInvoker(fileName);
    invoker.create(parameters);
    return invoker;
  }

  public Function<Object> get() {
    return invokeFunction;
  }

  public Object evaluate(GungnirTuple tuple) {
    return invokeFunction.getValue(tuple);
  }

  public Object exclude(GungnirTuple tuple) {
    if (invokeFunction instanceof ScriptAggregateFunction) {
      return ((ScriptAggregateFunction) invokeFunction).exclude(tuple);
    }
    return null;
  }

  public void clear() {
    if (invokeFunction instanceof ScriptAggregateFunction) {
      ((ScriptAggregateFunction) invokeFunction).clear();
    }
  }
}
