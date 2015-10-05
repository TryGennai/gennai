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

package org.gennai.gungnir.ql.analysis;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.FunctionEntity.FunctionType;
import org.gennai.gungnir.ql.FunctionEntity.ScriptType;
import org.gennai.gungnir.topology.udf.ArgumentException;
import org.gennai.gungnir.topology.udf.Average;
import org.gennai.gungnir.topology.udf.BaseFunction.Description;
import org.gennai.gungnir.topology.udf.Cast;
import org.gennai.gungnir.topology.udf.CollectList;
import org.gennai.gungnir.topology.udf.CollectSet;
import org.gennai.gungnir.topology.udf.Concat;
import org.gennai.gungnir.topology.udf.Cosine;
import org.gennai.gungnir.topology.udf.Count;
import org.gennai.gungnir.topology.udf.DateFormat;
import org.gennai.gungnir.topology.udf.Distance;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.topology.udf.Ifnull;
import org.gennai.gungnir.topology.udf.InvokeAggregateFunction;
import org.gennai.gungnir.topology.udf.InvokeFunction;
import org.gennai.gungnir.topology.udf.ParseUrl;
import org.gennai.gungnir.topology.udf.RegexpExtract;
import org.gennai.gungnir.topology.udf.ScriptAggregateFunction;
import org.gennai.gungnir.topology.udf.ScriptFunction;
import org.gennai.gungnir.topology.udf.Sine;
import org.gennai.gungnir.topology.udf.Size;
import org.gennai.gungnir.topology.udf.Slice;
import org.gennai.gungnir.topology.udf.Split;
import org.gennai.gungnir.topology.udf.Sqrt;
import org.gennai.gungnir.topology.udf.Sum;
import org.gennai.gungnir.topology.udf.Tangent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class FunctionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionRegistry.class);

  private FileRegistry fileRegistry;
  private Map<String, Function<?>> bifsMap = Maps.newHashMap();
  private Map<String, Function<?>> udfsMap = Maps.newHashMap();
  private MetaStore metaStore;

  public FunctionRegistry(FileRegistry fileRegistry) {
    this.fileRegistry = fileRegistry;

    register(Count.class);
    register(Average.class);
    register(Sum.class);
    register(Concat.class);
    register(Ifnull.class);
    register(Split.class);
    register(RegexpExtract.class);
    register(ParseUrl.class);
    register(Cast.class);
    register(DateFormat.class);
    register(Distance.class);
    register(Sqrt.class);
    register(Sine.class);
    register(Cosine.class);
    register(Tangent.class);
    register(Size.class);
    register(Slice.class);
    register(CollectList.class);
    register(CollectSet.class);
  }

  private void register(Class<? extends Function<?>> funcClass) {
    Description desc = funcClass.getAnnotation(Description.class);
    try {
      bifsMap.put(desc.name(), funcClass.newInstance());
    } catch (InstantiationException e) {
      LOG.error("Failed to create instance '{}' function", desc.name(), e);
    } catch (IllegalAccessException e) {
      LOG.error("Failed to create instance '{}' function", desc.name(), e);
    }
  }

  public void load(UserEntity owner) throws MetaStoreException {
    udfsMap.clear();

    try {
      fileRegistry.exportFiles(owner);
    } catch (IOException e) {
      LOG.error("Failed to load function", e);
    }
    String classPath = fileRegistry.getCacheDir();

    if (metaStore == null) {
      metaStore = GungnirManager.getManager().getMetaStore();
    }
    List<FunctionEntity> functions = metaStore.findFunctions(owner);
    for (FunctionEntity function : functions) {
      if (function.getLocation().endsWith(".class")) {
        if (function.getType() == FunctionType.UDF) {
          InvokeFunction invokeFunction = new InvokeFunction(function, classPath);
          udfsMap.put(function.getName(), invokeFunction);
        } else if (function.getType() == FunctionType.UDAF) {
          InvokeAggregateFunction invokeFunction = new InvokeAggregateFunction(function, classPath);
          udfsMap.put(function.getName(), invokeFunction);
        }
      } else {
        ScriptType scriptType = null;
        EnumSet<ScriptType> scriptTypes = EnumSet.allOf(ScriptType.class);
        for (ScriptType script : scriptTypes) {
          if (function.getLocation().endsWith(script.getExtension())) {
            scriptType = script;
            break;
          }
        }

        if (function.getType() == FunctionType.UDF) {
          ScriptFunction scriptFunction = new ScriptFunction(function, scriptType, classPath);
          udfsMap.put(function.getName(), scriptFunction);
        } else if (function.getType() == FunctionType.UDAF) {
          ScriptAggregateFunction scriptFunction = new ScriptAggregateFunction(function, scriptType,
              classPath);
          udfsMap.put(function.getName(), scriptFunction);
        }
      }
    }
  }

  public Function<?> create(String name, Object... parameters) throws SemanticAnalyzeException,
      SemanticAnalyzeException, ArgumentException {
    Function<?> func = udfsMap.get(name);
    if (func == null) {
      func = bifsMap.get(name);
    }

    if (func != null) {
      if (parameters != null) {
        return func.clone().create(parameters);
      } else {
        return func.clone().create();
      }
    } else {
      throw new SemanticAnalyzeException(name + " isn't registered");
    }
  }
}
